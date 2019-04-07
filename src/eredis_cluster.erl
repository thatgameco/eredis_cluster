-module(eredis_cluster).
-behaviour(application).

% Application.
-export([start/2]).
-export([stop/1]).

% API.
-export([start/0, stop/0, connect/2]). % Application Management.

% Generic redis call
-export([q/2, qp/2, qw/2, qk/3, qa/2, qmn/2, transaction/2, transaction/3]).

% Specific redis command implementation
-export([flushdb/1]).

% Pub/sub
-export([create_subscriber/2, stop_subscriber/1, subscribe/2, ack_message/1]).

 % Helper functions
-export([update_key/3]).
-export([update_hash_field/4]).
-export([optimistic_locking_transaction/4]).
-export([eval/5]).

-include("eredis_cluster.hrl").

% Application callback
-spec start(StartType::application:start_type(), StartArgs::term()) ->
    {ok, pid()}.
start(_Type, _Args) ->
    eredis_cluster_sup:start_link().

% Application callback
-spec stop(State::term()) -> ok.
stop(_State) ->
    ok.

% Convenient function for starting the application
-spec start() -> ok | {error, Reason::term()}.
start() ->
    application:start(?MODULE).

-spec stop() -> ok | {error, Reason::term()}.
stop() ->
    application:stop(?MODULE).

%% =============================================================================
%% @doc Connect to a set of init node, useful if the cluster configuration is
%% not known at startup
%% @end
%% =============================================================================
-spec connect(ClusterName::atom(), InitServers::term()) -> Result::term().
connect(ClusterName, InitServers) ->
    eredis_cluster_monitor:connect(ClusterName, InitServers).

%% =============================================================================
%% @doc Wrapper function to execute a pipeline command as a transaction Command
%% (it will add MULTI and EXEC command)
%% @end
%% =============================================================================
-spec transaction(ClusterName::atom(), redis_pipeline_command()) -> redis_transaction_result().
transaction(ClusterName, Commands) ->
    Result = q(ClusterName, [["multi"]| Commands] ++ [["exec"]]),
    lists:last(Result).

%% =============================================================================
%% @doc Execute a function on a pool worker. This function should be use when
%% transaction method such as WATCH or DISCARD must be used. The pool used to
%% execute the transaction is specified by giving a key that this pool is
%% containing.
%% @end
%% =============================================================================
-spec transaction(ClusterName::atom(), fun((Worker::pid()) -> redis_result()), anystring()) -> any().
transaction(ClusterName, Transaction, PoolKey) ->
    Slot = get_key_slot(PoolKey),
    transaction(ClusterName, Transaction, Slot, undefined, 0).

transaction(ClusterName, Transaction, Slot, undefined, _) ->
    query(ClusterName, Transaction, Slot, 0);
transaction(ClusterName, Transaction, Slot, ExpectedValue, Counter) ->
    case query(ClusterName, Transaction, Slot, 0) of
        ExpectedValue ->
            transaction(ClusterName, Transaction, Slot, ExpectedValue, Counter - 1);
        {ExpectedValue, _} ->
            transaction(ClusterName, Transaction, Slot, ExpectedValue, Counter - 1);
        Payload ->
            Payload
    end.

%% =============================================================================
%% @doc Multi node query
%% @end
%% =============================================================================
-spec qmn(ClusterName::atom(), redis_pipeline_command()) -> redis_pipeline_result().
qmn(ClusterName, Commands) -> qmn(ClusterName, Commands, 0).

qmn(_, _, ?REDIS_CLUSTER_REQUEST_TTL) -> 
    {error, no_connection};
qmn(ClusterName, Commands, Counter) ->
    %% Throttle retries
    throttle_retries(Counter),

    case split_by_pools(ClusterName, Commands) of
        {CommandsByPools, MappingInfo, Version} ->
            case qmn2(ClusterName, CommandsByPools, MappingInfo, [], Version) of
                retry -> qmn(ClusterName, Commands, Counter + 1);
                Res -> Res
            end;
        {retry, Version} ->
            eredis_cluster_monitor:refresh_mapping(ClusterName, Version),
            qmn(ClusterName, Commands, Counter + 1);
        {error, no_connection} ->
            {error, no_connection}
    end.

qmn2(ClusterName, [{Pool, PoolCommands} | T1], [{Pool, Mapping} | T2], Acc, Version) ->
    Transaction = fun(Worker) -> qw(Worker, PoolCommands) end,
    Result = eredis_cluster_pool:transaction(Pool, Transaction),
    case handle_transaction_result(ClusterName, Result, Version, check_pipeline_result) of
        retry -> retry;
        Res -> 
            MappedRes = lists:zip(Mapping,Res),
            qmn2(ClusterName, T1, T2, MappedRes ++ Acc, Version)
    end;
qmn2(_ClusterName, [], [], Acc, _) ->
    SortedAcc =
        lists:sort(
            fun({Index1, _},{Index2, _}) ->
                Index1 < Index2
            end, Acc),
    [Res || {_,Res} <- SortedAcc].

split_by_pools(ClusterName, Commands) ->
    State = eredis_cluster_monitor:get_state(ClusterName),
    case State of 
        {error, empty_state} -> {error, no_connection};
        _ -> split_by_pools(Commands, 1, [], [], State)
    end.

split_by_pools([Command | T], Index, CmdAcc, MapAcc, State) ->
    Key = get_key_from_command(Command),
    Slot = get_key_slot(Key),
    {Pool, Version} = eredis_cluster_monitor:get_pool_by_slot(state, State, Slot),
    case Pool of
        {error, missing_slots} ->
            % Retry when slots are missing
            {retry, Version};
        Pool ->
            {NewAcc1, NewAcc2} =
                case lists:keyfind(Pool, 1, CmdAcc) of
                    false ->
                        {[{Pool, [Command]} | CmdAcc], [{Pool, [Index]} | MapAcc]};
                    {Pool, CmdList} ->
                        CmdList2 = [Command | CmdList],
                        CmdAcc2  = lists:keydelete(Pool, 1, CmdAcc),
                        {Pool, MapList} = lists:keyfind(Pool, 1, MapAcc),
                        MapList2 = [Index | MapList],
                        MapAcc2  = lists:keydelete(Pool, 1, MapAcc),
                        {[{Pool, CmdList2} | CmdAcc2], [{Pool, MapList2} | MapAcc2]}
                end,
            split_by_pools(T, Index+1, NewAcc1, NewAcc2, State)
    end;
split_by_pools([], _Index, CmdAcc, MapAcc, State) ->
    CmdAcc2 = [{Pool, lists:reverse(Commands)} || {Pool, Commands} <- CmdAcc],
    MapAcc2 = [{Pool, lists:reverse(Mapping)} || {Pool, Mapping} <- MapAcc],
    {CmdAcc2, MapAcc2, eredis_cluster_monitor:get_state_version(State)}.

%% =============================================================================
%% @doc Wrapper function for command using pipelined commands
%% @end
%% =============================================================================
-spec qp(ClusterName::atom(), redis_pipeline_command()) -> redis_pipeline_result().
qp(ClusterName, Commands) -> q(ClusterName, Commands).

%% =============================================================================
%% @doc This function execute simple or pipelined command on a single redis node
%% the node will be automatically found according to the key used in the command
%% @end
%% =============================================================================
-spec q(ClusterName::atom(), redis_command()) -> redis_result().
q(ClusterName, Command) ->
    query(ClusterName, Command).

-spec qk(ClusterName::atom(), redis_command(), bitstring()) -> redis_result().
qk(ClusterName, Command, PoolKey) ->
    query(ClusterName, Command, PoolKey).

query(ClusterName, Command) ->
    PoolKey = get_key_from_command(Command),
    query(ClusterName, Command, PoolKey).

query(_, _, undefined) ->
    {error, invalid_cluster_command};
query(ClusterName, Command, PoolKey) ->
    Slot = get_key_slot(PoolKey),
    Transaction = fun(Worker) -> qw(Worker, Command) end,
    query(ClusterName, Transaction, Slot, 0).

query(_, _, _, ?REDIS_CLUSTER_REQUEST_TTL) ->
    {error, no_connection};
query(ClusterName, Transaction, Slot, Counter) ->
    %% Throttle retries
    throttle_retries(Counter),

    {Pool, Version} = eredis_cluster_monitor:get_pool_by_slot(cluster_name, ClusterName, Slot),

    case Pool of
        {error, missing_slots} ->
            % Retry when slots are missing
            eredis_cluster_monitor:refresh_mapping(ClusterName, Version),
            query(ClusterName, Transaction, Slot, Counter + 1);
        Pool ->
            Result = eredis_cluster_pool:transaction(Pool, Transaction),
            case handle_transaction_result(ClusterName, Result, Version) of 
                retry -> query(ClusterName, Transaction, Slot, Counter + 1);
                Result -> Result
            end
    end.

handle_transaction_result(ClusterName, Result, Version) ->
    case Result of 
       % If we detect a node went down, we should probably refresh the slot
        % mapping.
        {error, no_connection} ->
            eredis_cluster_monitor:refresh_mapping(ClusterName, Version),
            retry;

        % If the tcp connection is closed (connection timeout), the redis worker
        % will try to reconnect, thus the connection should be recovered for
        % the next request. We don't need to refresh the slot mapping in this
        % case
        {error, tcp_closed} ->
            retry;

        % Redis explicitly say our slot mapping is incorrect, we need to refresh
        % it
        {error, <<"MOVED ", _/binary>>} ->
            eredis_cluster_monitor:refresh_mapping(ClusterName, Version),
            retry;

        Payload ->
            Payload
    end.
handle_transaction_result(ClusterName, Result, Version, check_pipeline_result) ->
    case handle_transaction_result(ClusterName, Result, Version) of
       retry -> retry;
       Payload when is_list(Payload) ->
           Pred = fun({error, <<"MOVED ", _/binary>>}) -> true;
                    (_) -> false
                 end,
           case lists:any(Pred, Payload) of
               false -> Payload;
               true ->
                   eredis_cluster_monitor:refresh_mapping(ClusterName, Version),
                   retry
           end;
       Payload -> Payload
    end.

-spec throttle_retries(integer()) -> ok.
throttle_retries(0) -> ok;
throttle_retries(_) -> timer:sleep(?REDIS_RETRY_DELAY).

%% =============================================================================
%% @doc Update the value of a key by applying the function passed in the
%% argument. The operation is done atomically
%% @end
%% =============================================================================
-spec update_key(ClusterName::atom(), Key::anystring(), UpdateFunction::fun((any()) -> any())) ->
    redis_transaction_result().
update_key(ClusterName, Key, UpdateFunction) ->
    UpdateFunction2 = fun(GetResult) ->
        {ok, Var} = GetResult,
        UpdatedVar = UpdateFunction(Var),
        {[["SET", Key, UpdatedVar]], UpdatedVar}
    end,
    case optimistic_locking_transaction(ClusterName, Key, ["GET", Key], UpdateFunction2) of
        {ok, {_, NewValue}} ->
            {ok, NewValue};
        Error ->
            Error
    end.

%% =============================================================================
%% @doc Update the value of a field stored in a hash by applying the function
%% passed in the argument. The operation is done atomically
%% @end
%% =============================================================================
-spec update_hash_field(ClusterName::atom(), Key::anystring(), Field::anystring(),
    UpdateFunction::fun((any()) -> any())) -> redis_transaction_result().
update_hash_field(ClusterName, Key, Field, UpdateFunction) ->
    UpdateFunction2 = fun(GetResult) ->
        {ok, Var} = GetResult,
        UpdatedVar = UpdateFunction(Var),
        {[["HSET", Key, Field, UpdatedVar]], UpdatedVar}
    end,
    case optimistic_locking_transaction(ClusterName, Key, ["HGET", Key, Field], UpdateFunction2) of
        {ok, {[FieldPresent], NewValue}} ->
            {ok, {FieldPresent, NewValue}};
        Error ->
            Error
    end.

%% =============================================================================
%% @doc Optimistic locking transaction helper, based on Redis documentation :
%% http://redis.io/topics/transactions
%% @end
%% =============================================================================
-spec optimistic_locking_transaction(ClusterName::atom(), Key::anystring(), redis_command(),
    UpdateFunction::fun((redis_result()) -> redis_pipeline_command())) ->
        {redis_transaction_result(), any()}.
optimistic_locking_transaction(ClusterName, WatchedKey, GetCommand, UpdateFunction) ->
    Slot = get_key_slot(WatchedKey),
    Transaction = fun(Worker) ->
        %% Watch given key
        qw(Worker,["WATCH", WatchedKey]),
        %% Get necessary information for the modifier function
        GetResult = qw(Worker, GetCommand),
        %% Execute the pipelined command as a redis transaction
        {UpdateCommand, Result} = case UpdateFunction(GetResult) of
            {Command, Var} ->
                {Command, Var};
            Command ->
                {Command, undefined}
        end,
        RedisResult = qw(Worker, [["MULTI"]] ++ UpdateCommand ++ [["EXEC"]]),
        {lists:last(RedisResult), Result}
    end,
	case transaction(ClusterName, Transaction, Slot, {ok, undefined}, ?OL_TRANSACTION_TTL) of
        {{ok, undefined}, _} ->
            {error, resource_busy};
        {{ok, TransactionResult}, UpdateResult} ->
            {ok, {TransactionResult, UpdateResult}};
        {Error, _} ->
            Error
    end.

%% =============================================================================
%% @doc Eval command helper, to optimize the query, it will try to execute the
%% script using its hashed value. If no script is found, it will load it and
%% try again.
%% It uses the first key to decide which node to run the script on. If Keys are
%% empty it'll connect to a random node. 
%% If ScriptHash is undefined, it'll load it and return the hash value. 
%% @end
%% =============================================================================
-spec eval(ClusterName::atom(), Script::bitstring(), ScriptHash::bitstring(), Keys::[bitstring()], Args::[bitstring()]) ->
    {ScriptHash::(unchanged | bitstring()), redis_result()}.
eval(ClusterName, Script, ScriptHash, Keys, Args) ->
    KeyNb = length(Keys),
    LoadCommand = ["SCRIPT", "LOAD", Script],
    PoolKey = if
        KeyNb == 0 -> random_key(8); %Random key
        true -> hd(Keys)
    end,

    EvalShaCommand = fun(Hash) ->
        ["EVALSHA", Hash, KeyNb] ++ Keys ++ Args
    end,

    LoadAndEval = fun() ->
        case qk(ClusterName, LoadCommand, PoolKey) of
            {ok, NewScriptHash} ->
                Result = qk(ClusterName, EvalShaCommand(NewScriptHash), PoolKey),
                {NewScriptHash, Result};
            {error, Reason} ->
                {undefined, {error, Reason}}
        end
    end,

    case ScriptHash of
        undefined -> LoadAndEval();
        S -> 
            case qk(ClusterName, EvalShaCommand(S), PoolKey) of
                {error, <<"NOSCRIPT", _/binary>>} ->
                    LoadAndEval();
                Result ->
                    {unchanged, Result}
            end
    end.

random_key(N) -> 
    % Random characters from ' ' to 'z'; the hashtag indicator '{' (123) '}' (125) not included. 
    [crypto:rand_uniform(40, 172) || _ <- lists:seq(1,N)]. 

%% =============================================================================
%% @doc Perform a given query on all node of a redis cluster
%% @end
%% =============================================================================
-spec qa(ClusterName::atom(), redis_command()) -> ok | {error, Reason::bitstring()}.
qa(ClusterName, Command) ->
    Pools = eredis_cluster_monitor:get_all_pools(ClusterName),
    Transaction = fun(Worker) -> qw(Worker, Command) end,
    [eredis_cluster_pool:transaction(Pool, Transaction) || Pool <- Pools].

%% =============================================================================
%% @doc Wrapper function to be used for direct call to a pool worker in the
%% function passed to the transaction/2 method
%% @end
%% =============================================================================
-spec qw(Worker::pid(), redis_command()) -> redis_result().
qw(Worker, Command) ->
    eredis_cluster_pool_worker:query(Worker, Command).

%% =============================================================================
%% @doc Perform flushdb command on each node of the redis cluster
%% @end
%% =============================================================================
-spec flushdb(ClusterName::atom()) -> ok | {error, Reason::bitstring()}.
flushdb(ClusterName) ->
    Result = qa(ClusterName, ["FLUSHDB"]),
    case proplists:lookup(error,Result) of
        none ->
            ok;
        Error ->
            Error
    end.

%% =============================================================================
%% @doc Return the hash slot from the key
%% @end
%% =============================================================================
-spec get_key_slot(Key::anystring()) -> Slot::integer().
get_key_slot(Key) when is_bitstring(Key) ->
    get_key_slot(bitstring_to_list(Key));
get_key_slot(Key) ->
    KeyToBeHased = case string:chr(Key,${) of
        0 ->
            Key;
        Start ->
            case string:chr(string:substr(Key,Start+1),$}) of
                0 ->
                    Key;
                Length ->
                    if
                        Length =:= 1 ->
                            Key;
                        true ->
                            string:substr(Key,Start+1,Length-1)
                    end
            end
    end,
    eredis_cluster_hash:hash(KeyToBeHased).

%% =============================================================================
%% @doc Return the first key in the command arguments.
%% In a normal query, the second term will be returned
%%
%% If it is a pipeline query we will use the second term of the first term, we
%% will assume that all keys are in the same server and the query can be
%% performed
%%
%% If the pipeline query starts with multi (transaction), we will look at the
%% second term of the second command
%%
%% For eval and evalsha command we will look at the fourth term.
%%
%% For commands that don't make sense in the context of cluster
%% return value will be undefined.
%% @end
%% =============================================================================
-spec get_key_from_command(redis_command()) -> string() | undefined.
get_key_from_command([[X|Y]|Z]) when is_bitstring(X) ->
    get_key_from_command([[bitstring_to_list(X)|Y]|Z]);
get_key_from_command([[X|Y]|Z]) when is_list(X) ->
    case string:to_lower(X) of
        "multi" ->
            get_key_from_command(Z);
        _ ->
            get_key_from_command([X|Y])
    end;
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term1) ->
    get_key_from_command([bitstring_to_list(Term1),Term2|Rest]);
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term2) ->
    get_key_from_command([Term1,bitstring_to_list(Term2)|Rest]);
get_key_from_command([Term1,Term2|Rest]) ->
    case string:to_lower(Term1) of
        "info" ->
            undefined;
        "config" ->
            undefined;
        "shutdown" ->
            undefined;
        "slaveof" ->
            undefined;
        "eval" ->
            get_key_from_rest(Rest);
        "evalsha" ->
            get_key_from_rest(Rest);
        _ ->
            Term2
    end;
get_key_from_command(_) ->
    undefined.

%% =============================================================================
%% @doc Get key for command where the key is in th 4th position (eval and
%% evalsha commands)
%% @end
%% =============================================================================
-spec get_key_from_rest([anystring()]) -> string() | undefined.
get_key_from_rest([_,KeyName|_]) when is_bitstring(KeyName) ->
    bitstring_to_list(KeyName);
get_key_from_rest([_,KeyName|_]) when is_list(KeyName) ->
    KeyName;
get_key_from_rest(_) ->
    undefined.

%% =============================================================================
%% Pub-sub support on non-cluster node
%% =============================================================================
%% @doc Create a new eredis_sub client and connect to a non-clustered server. See eredis_sub module for more details.
%% @note This function will fail if the given server name has cluster enabled. 
-spec create_subscriber(SingleServerName::atom(), ControllingProcess::pid()) -> pid().
create_subscriber(SingleServerName, ControllingProcess) ->
    [SingleNode] = eredis_cluster_monitor:get_all_nodes(SingleServerName),

    #node{address = Address, port = Port} = SingleNode,
    Password = application:get_env(eredis_cluster, password, ""),

    % TODO: ReconnectSleep, MaxQueueSize, QueueBehavior
    {ok, Sub} = eredis_sub:start_link(Address, Port, Password, 1000, infinity, drop),
    eredis_sub:controlling_process(Sub, ControllingProcess),
    Sub.

%% @doc: Subscribe to the given channels. Returns immediately. The
%% result will be delivered to the controlling process as any other
%% message. Delivers {subscribed, Channel::binary(), pid()}
-spec subscribe(Sub::pid(), Channels::[atom() | binary()]) -> ok.
subscribe(Sub, Channels) ->
    eredis_sub:subscribe(Sub, Channels).

%% @doc Stop the specified eredis_sub. Will unsubscribe all channels before stoppping.
-spec stop_subscriber(Sub::pid()) -> ok.
stop_subscriber(Sub) ->
    {ok, Channels} = eredis_sub:channels(Sub),
    eredis_sub:unsubscribe(Sub, Channels),
    eredis_sub:stop(Sub).

%% @doc: acknowledge the receipt of a pubsub message. each pubsub
%% message must be acknowledged before the next one is received
-spec ack_message(Sub::pid()) -> ok.
ack_message(Sub) ->
    eredis_sub:ack_message(Sub).