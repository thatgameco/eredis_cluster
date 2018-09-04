-module(eredis_cluster_monitor).
-behaviour(gen_server).

%% API.
-export([start_link/0]).
-export([connect/2]).
-export([refresh_mapping/2]).
-export([get_state/1, get_state_version/1]).
-export([get_pool_by_slot/3]).
-export([get_all_pools/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% Type definition.
-include("eredis_cluster.hrl").
-record(state, {
    cluster_name :: atom(),
    init_nodes :: [#node{}],
    slots :: tuple(), %% whose elements are integer indexes into slots_maps
    slots_maps :: tuple(), %% whose elements are #slots_map{}
    version :: integer()
}).

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

connect(ClusterName, InitServers) ->
    gen_server:call(?MODULE,{connect, ClusterName, InitServers}).

refresh_mapping(ClusterName, Version) ->
    gen_server:call(?MODULE,{reload_slots_map, ClusterName, Version}).

%% =============================================================================
%% @doc Given a slot return the link (Redis instance) to the mapped
%% node.
%% @end
%% =============================================================================

-spec get_state(ClusterName::atom()) -> #state{} | {error, Reason::atom()}.
get_state(ClusterName) ->
    case ets:lookup(?MODULE, {ClusterName, cluster_state}) of
        [] -> 
            {error, empty_state};
        [{{ClusterName, cluster_state}, State}] -> 
            State;
        _ ->
            {error, unknown_state}
    end.

get_state_version(State) ->
    State#state.version.

-spec get_all_pools(ClusterName::atom()) -> [pid()].
get_all_pools(ClusterName) ->
    State = get_state(ClusterName),
    SlotsMapList = tuple_to_list(State#state.slots_maps),
    [SlotsMap#slots_map.node#node.pool || SlotsMap <- SlotsMapList,
        SlotsMap#slots_map.node =/= undefined].

%% =============================================================================
%% @doc Get cluster pool by slot. Optionally, a memoized State can be provided
%% to prevent from querying ets inside loops.
%% @end
%% =============================================================================
-spec get_pool_by_slot(atom(), #state{} | atom(), Slot::integer()) ->
    {PoolName::atom() | undefined | {error, Reason::atom()}, Version::integer()}.
get_pool_by_slot(state, State, Slot) -> 
    case State of
        State when is_record(State, state) and (tuple_size(State#state.slots) > Slot) ->
            Index = element(Slot+1,State#state.slots),
            Cluster = element(Index,State#state.slots_maps),
            case Cluster#slots_map.node of 
                Node when is_record(Node, node) -> {Node#node.pool, State#state.version};
                _ -> {undefined, State#state.version}
            end;
        State when is_record(State, state) ->
            {{error, missing_slots}, get_state_version(State)};
        {error, Reason} ->
            {{error, Reason}, 0}
    end;
get_pool_by_slot(cluster_name, ClusterName, Slot) ->
    State = get_state(ClusterName),
    get_pool_by_slot(state, State, Slot).

-spec reload_slots_map(State::#state{}) -> NewState::#state{}.
reload_slots_map(State) ->
    [close_connection(SlotsMap)
        || SlotsMap <- tuple_to_list(State#state.slots_maps)],

    ClusterSlots = get_cluster_slots(State#state.init_nodes),

    SlotsMaps = parse_cluster_slots(ClusterSlots),
    ConnectedSlotsMaps = connect_all_slots(SlotsMaps),
    Slots = create_slots_cache(ConnectedSlotsMaps),

    ClusterName = State#state.cluster_name,
    NewState = State#state{
        slots = list_to_tuple(Slots),
        slots_maps = list_to_tuple(ConnectedSlotsMaps),
        version = State#state.version + 1,
        cluster_name = ClusterName
    },

    true = ets:insert(?MODULE, [{{ClusterName, cluster_state}, NewState}]),

    NewState.

-spec get_cluster_slots([#node{}]) -> [[bitstring() | [bitstring()]]].
get_cluster_slots([]) ->
    throw({error,cannot_connect_to_cluster});
get_cluster_slots([Node|T]) ->
    case safe_eredis_start_link(Node#node.address, Node#node.port) of
        {ok,Connection} ->
          case eredis:q(Connection, ["CLUSTER", "SLOTS"]) of
            {error,<<"ERR unknown command 'CLUSTER'">>} ->
                get_cluster_slots_from_single_node(Node);
            {error,<<"ERR This instance has cluster support disabled">>} ->
                get_cluster_slots_from_single_node(Node);
            {ok, ClusterInfo} ->
                eredis:stop(Connection),
                ClusterInfo;
            _ ->
                eredis:stop(Connection),
                get_cluster_slots(T)
        end;
        _ ->
            get_cluster_slots(T)
  end.

-spec get_cluster_slots_from_single_node(#node{}) ->
    [[bitstring() | [bitstring()]]].
get_cluster_slots_from_single_node(Node) ->
    [[<<"0">>, integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
    [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]]].

-spec parse_cluster_slots([[bitstring() | [bitstring()]]]) -> [#slots_map{}].
parse_cluster_slots(ClusterInfo) ->
    parse_cluster_slots(ClusterInfo, 1, []).

parse_cluster_slots([[StartSlot, EndSlot | [[Address, Port | _] | _]] | T], Index, Acc) ->
    SlotsMap =
        #slots_map{
            index = Index,
            start_slot = binary_to_integer(StartSlot),
            end_slot = binary_to_integer(EndSlot),
            node = #node{
                address = binary_to_list(Address),
                port = binary_to_integer(Port)
            }
        },
    parse_cluster_slots(T, Index+1, [SlotsMap | Acc]);
parse_cluster_slots([], _Index, Acc) ->
    lists:reverse(Acc).



-spec close_connection(#slots_map{}) -> ok.
close_connection(SlotsMap) ->
    Node = SlotsMap#slots_map.node,
    if
        Node =/= undefined ->
            try eredis_cluster_pool:stop(Node#node.pool) of
                _ ->
                    ok
            catch
                _ ->
                    ok
            end;
        true ->
            ok
    end.

-spec connect_node(#node{}) -> #node{} | undefined.
connect_node(Node) ->
    case eredis_cluster_pool:create(Node#node.address, Node#node.port) of
        {ok, Pool} ->
            Node#node{pool=Pool};
        _ ->
            undefined
    end.

safe_eredis_start_link(Address,Port) ->
    process_flag(trap_exit, true),
    DataBase = application:get_env(eredis_cluster, database, 0),
    Password = application:get_env(eredis_cluster, password, ""),
    Payload = eredis:start_link(Address, Port, DataBase, Password),
    process_flag(trap_exit, false),
    Payload.

-spec create_slots_cache([#slots_map{}]) -> [integer()].
create_slots_cache(SlotsMaps) ->
  SlotsCache = [[{Index,SlotsMap#slots_map.index}
        || Index <- lists:seq(SlotsMap#slots_map.start_slot,
            SlotsMap#slots_map.end_slot)]
        || SlotsMap <- SlotsMaps],
  SlotsCacheF = lists:flatten(SlotsCache),
  SortedSlotsCache = lists:sort(SlotsCacheF),
  [ Index || {_,Index} <- SortedSlotsCache].

-spec connect_all_slots([#slots_map{}]) -> [integer()].
connect_all_slots(SlotsMapList) ->
    [SlotsMap#slots_map{node=connect_node(SlotsMap#slots_map.node)}
        || SlotsMap <- SlotsMapList].

-spec connect_(ClusterName::atom(), [{Address::string(), Port::integer()}]) -> #state{}.
connect_(ClusterName, []) ->
    #state{cluster_name = ClusterName};
connect_(ClusterName, InitNodes) ->
    State = #state{
        slots = undefined,
        slots_maps = {},
        init_nodes = [#node{address = A, port = P} || {A,P} <- InitNodes],
        version = 0,
        cluster_name = ClusterName
    },

    reload_slots_map(State).

%% gen_server.

init(_Args) ->
    ets:new(?MODULE, [protected, set, named_table, {read_concurrency, true}]),

    InitNodes = application:get_env(eredis_cluster, init_nodes, []),

    try 
        ClusterStatesList = lists:map(fun({ClusterName, InitServer}) -> 
            {ClusterName, connect_(ClusterName, [InitServer])}
        end, InitNodes),
        {ok, maps:from_list(ClusterStatesList)}
    catch 
        {error,cannot_connect_to_cluster} -> 
            % Do not crash and retry immediately; instead, wait for couple seconds for the cluster to recover
            RetryInterval = application:get_env(eredis_cluster, retry_interval_ms, 3000),
            timer:sleep(RetryInterval),
            {stop, cannot_connect_to_cluster}
    end.

handle_call({reload_slots_map, ClusterName, Version}, _From, ClusterStates) ->
    case maps:find(ClusterName, ClusterStates) of
        {ok, State} when Version == State#state.version -> 
            NewState = reload_slots_map(State),
            {reply, ok, maps:update(ClusterName, NewState, ClusterStates)};
        {ok, _} -> 
            {reply, ok, ClusterStates};
        _ ->
            {reply, {error, ClusterName, not_connected}, ClusterStates}
    end;
handle_call({connect, ClusterName, InitServers}, _From, ClusterStates) ->
    NewState = connect_(ClusterName, InitServers),
    {reply, ok, maps:put(ClusterName, NewState, ClusterStates)};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
