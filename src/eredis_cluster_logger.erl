-module(eredis_cluster_logger).
-behaviour(gen_server).

-export([info/1, info/2, error/1, error/2]).
-export([set_logger/2]).

-export([start_link/0]).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).
-export([default_log_fun/2]).

-record(state, {
    info_logger :: {module(), fun()},
    error_logger :: {module(), fun()}
}).

set_logger({InfoModule, InfoFun}, {ErrorModule, ErrorFun}) ->
    gen_server:call(?MODULE, {set_logger, {InfoModule, InfoFun}, {ErrorModule, ErrorFun}}).

info(Format) -> eredis_cluster_logger:info(Format, []).
info(Format, Args) ->
    gen_server:cast(?MODULE, {info, Format, Args}).

error(Format) -> eredis_cluster_logger:error(Format, []).
error(Format, Args) ->
    gen_server:cast(?MODULE, {error, Format, Args}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    InfoLogger = application:get_env(eredis_cluster, info_logger, {?MODULE, default_log_fun}),
    ErrorLogger = application:get_env(eredis_cluster, error_logger, {?MODULE, default_log_fun}),

    {ok, #state{
        info_logger = InfoLogger,
        error_logger = ErrorLogger
    }}.

handle_call({set_logger, {InfoModule, InfoFun}, {ErrorModuel, ErrorFun}}, _From, _State) ->
    {ok, #state{
        info_logger = {InfoModule, InfoFun},
        error_logger = {ErrorModuel, ErrorFun}
    }};

handle_call(_Query, _From, State) ->
    {reply, ok, State}.

handle_cast({info, Format, Args}, #state{info_logger = {Module, Fun}} = State) ->
    erlang:apply(Module, Fun, [Format, Args]),
    {noreply, State};

handle_cast({error, Format, Args}, #state{error_logger = {Module, Fun}} = State) ->
    erlang:apply(Module, Fun, [Format, Args]),
    {noreply, State};

handle_cast(_Query, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

default_log_fun(Format, Args) ->
    io:format("eredis_cluster: " ++ Format ++ "~n", Args).