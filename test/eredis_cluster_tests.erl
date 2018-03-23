-module(eredis_cluster_tests).

-include_lib("eunit/include/eunit.hrl").

-define(Setup, fun() -> eredis_cluster:start(), eredis_cluster:connect(c1, [{"127.0.0.1", 30001}]) end).
-define(Cleanup, fun(_) -> eredis_cluster:stop() end).

-define(SetupMulti, fun() -> 
    eredis_cluster:start(), 
    eredis_cluster:connect(c1, [ {"127.0.0.1", 30001} ]),
    eredis_cluster:connect(c2, [ {"127.0.0.1", 31001} ]),
    eredis_cluster:connect(c3, [ {"127.0.0.1", 32001} ])
end).

basic_test_() ->
    {inorder,
        {setup, ?Setup, ?Cleanup,
        [
            { "get and set",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(c1, ["SET", "key", "value"])),
                ?assertEqual({ok, <<"value">>}, eredis_cluster:q(c1, ["GET","key"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(c1, ["GET","nonexists"]))
            end
            },

            { "binary",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(c1, [<<"SET">>, <<"key_binary">>, <<"value_binary">>])),
                ?assertEqual({ok, <<"value_binary">>}, eredis_cluster:q(c1, [<<"GET">>,<<"key_binary">>])),
                ?assertEqual([{ok, <<"value_binary">>},{ok, <<"value_binary">>}], eredis_cluster:qp(c1, [[<<"GET">>,<<"key_binary">>],[<<"GET">>,<<"key_binary">>]]))
            end
            },

            { "delete test",
            fun() ->
                ?assertMatch({ok, _}, eredis_cluster:q(c1, ["DEL", "a"])),
                ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(c1, ["SET", "b", "a"])),
                ?assertEqual({ok, <<"1">>}, eredis_cluster:q(c1, ["DEL", "b"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(c1, ["GET", "b"]))
            end
            },

            { "pipeline",
            fun () ->
                ?assertNotMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp(c1, [["SET", "a1", "aaa"], ["SET", "a2", "aaa"], ["SET", "a3", "aaa"]])),
                ?assertMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp(c1, [["LPUSH", "a", "aaa"], ["LPUSH", "a", "bbb"], ["LPUSH", "a", "ccc"]]))
            end
            },

            { "multi node get",
                fun () ->
                    N=1000,
                    Keys = [integer_to_list(I) || I <- lists:seq(1,N)],
                    [eredis_cluster:q(c1, ["SETEX", Key, "50", Key]) || Key <- Keys],
                    Guard1 = [{ok, integer_to_binary(list_to_integer(Key))} || Key <- Keys],
                    ?assertMatch(Guard1, eredis_cluster:qmn(c1, [["GET", Key] || Key <- Keys])),
                    eredis_cluster:q(c1, ["SETEX", "a", "50", "0"]),
                    Guard2 = [{ok, integer_to_binary(0)} || _Key <- lists:seq(1,5)],
                    ?assertMatch(Guard2, eredis_cluster:qmn(c1, [["GET", "a"] || _I <- lists:seq(1,5)]))
                end
            },

            % WARNING: This test will fail during rebalancing, as qmn does not guarantee transaction across nodes
            { "multi node",
                fun () ->
                    N=1000,
                    Keys = [integer_to_list(I) || I <- lists:seq(1,N)],
                    [eredis_cluster:q(c1, ["SETEX", Key, "50", Key]) || Key <- Keys],
                    Guard1 = [{ok, integer_to_binary(list_to_integer(Key)+1)} || Key <- Keys],
                    ?assertMatch(Guard1, eredis_cluster:qmn(c1, [["INCR", Key] || Key <- Keys])),
                    eredis_cluster:q(c1, ["SETEX", "a", "50", "0"]),
                    Guard2 = [{ok, integer_to_binary(Key)} || Key <- lists:seq(1,5)],
                    ?assertMatch(Guard2, eredis_cluster:qmn(c1, [["INCR", "a"] || _I <- lists:seq(1,5)]))
                end
            },

            { "transaction",
            fun () ->
                ?assertMatch({ok,[_,_,_]}, eredis_cluster:transaction(c1, [["get","abc"],["get","abc"],["get","abc"]])),
                ?assertMatch({error,_}, eredis_cluster:transaction(c1, [["get","abc"],["get","abcde"],["get","abcd1"]]))
            end
            },

            { "function transaction",
            fun () ->
                eredis_cluster:q(c1, ["SET", "efg", "12"]),
                Function = fun(Worker) ->
                    eredis_cluster:qw(Worker, ["WATCH", "efg"]),
                    {ok, Result} = eredis_cluster:qw(Worker, ["GET", "efg"]),
                    NewValue = binary_to_integer(Result) + 1,
                    timer:sleep(100),
                    lists:last(eredis_cluster:qw(Worker, [["MULTI"],["SET", "efg", NewValue],["EXEC"]]))
                end,
                PResult = rpc:pmap({eredis_cluster, transaction},[Function, "efg"],lists:duplicate(5, c1)),
                Nfailed = lists:foldr(fun({_, Result}, Acc) -> if Result == undefined -> Acc + 1; true -> Acc end end, 0, PResult),
                ?assertEqual(4, Nfailed)
            end
            },

            { "eval key",
            fun () ->
                eredis_cluster:q(c1, ["del", "foo"]),
                eredis_cluster:q(c1, ["eval","return redis.call('set',KEYS[1],'bar')", "1", "foo"]),
                ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(c1, ["GET", "foo"]))
            end
            },

            { "evalsha",
            fun () ->
                % In this test the key "load" will be used because the "script
                % load" command will be executed in the redis server containing
                % the "load" key. The script should be propagated to other redis
                % client but for some reason it is not done on Travis test
                % environment. @TODO : fix travis redis cluster configuration,
                % or give the possibility to run a command on an arbitrary
                % redis server (no slot derived from key name)
                eredis_cluster:q(c1, ["del", "load"]),
                {ok, Hash} = eredis_cluster:q(c1, ["script","load","return redis.call('set',KEYS[1],'bar')"]),
                eredis_cluster:q(c1, ["evalsha", Hash, 1, "load"]),
                ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(c1, ["GET", "load"]))
            end
            },

            { "bitstring support",
            fun () ->
                eredis_cluster:q(c1, [<<"set">>, <<"bitstring">>,<<"support">>]),
                ?assertEqual({ok, <<"support">>}, eredis_cluster:q(c1, [<<"GET">>, <<"bitstring">>]))
            end
            },

            { "flushdb",
            fun () ->
                eredis_cluster:q(c1, ["set", "zyx", "test"]),
                eredis_cluster:q(c1, ["set", "zyxw", "test"]),
                eredis_cluster:q(c1, ["set", "zyxwv", "test"]),
                eredis_cluster:flushdb(c1),
                ?assertEqual({ok, undefined}, eredis_cluster:q(c1, ["GET", "zyx"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(c1, ["GET", "zyxw"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(c1, ["GET", "zyxwv"]))
            end
            },

            { "atomic get set",
            fun () ->
                eredis_cluster:q(c1, ["set", "hij", 2]),
                Incr = fun(Var) -> binary_to_integer(Var) + 1 end,
                Result = rpc:pmap({eredis_cluster, update_key}, ["hij", Incr], lists:duplicate(5, c1)),
                IntermediateValues = proplists:get_all_values(ok, Result),
                ?assertEqual([3,4,5,6,7], lists:sort(IntermediateValues)),
                ?assertEqual({ok, <<"7">>}, eredis_cluster:q(c1, ["get", "hij"]))
            end
            },

            { "atomic hget hset",
            fun () ->
                eredis_cluster:q(c1, ["hset", "klm", "nop", 2]),
                Incr = fun(Var) -> binary_to_integer(Var) + 1 end,
                Result = rpc:pmap({eredis_cluster, update_hash_field}, ["klm", "nop", Incr], lists:duplicate(5, c1)),
                IntermediateValues = proplists:get_all_values(ok, Result),
                ?assertEqual([{<<"0">>,3},{<<"0">>,4},{<<"0">>,5},{<<"0">>,6},{<<"0">>,7}], lists:sort(IntermediateValues)),
                ?assertEqual({ok, <<"7">>}, eredis_cluster:q(c1, ["hget", "klm", "nop"]))
            end
            },

            { "eval",
            fun () ->
                Script = <<"return redis.call('set', KEYS[1], ARGV[1]);">>,
                ScriptHash = << << if N >= 10 -> N -10 + $a; true -> N + $0 end >> || <<N:4>> <= crypto:hash(sha, Script) >>,
                eredis_cluster:eval(c1, Script, ScriptHash, ["qrs"], ["evaltest"]),
                ?assertEqual({ok, <<"evaltest">>}, eredis_cluster:q(c1, ["get", "qrs"]))
            end
            }
      ]
    }
}.

multi_node_test_() -> 
{   inorder,
    {setup, ?SetupMulti, ?Cleanup, [
        { "get and set",
        fun() ->
            ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(c1, ["SET", "key", "value1"])),
            ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(c2, ["SET", "key", "value2"])),
            ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(c3, ["SET", "key", "value3"])),
            ?assertEqual({ok, <<"value1">>}, eredis_cluster:q(c1, ["GET","key"])),
            ?assertEqual({ok, <<"value2">>}, eredis_cluster:q(c2, ["GET","key"])),
            ?assertEqual({ok, <<"value3">>}, eredis_cluster:q(c3, ["GET","key"])),
            ?assertEqual({ok, undefined}, eredis_cluster:q(c1, ["GET","nonexists"])),
            ?assertEqual({ok, undefined}, eredis_cluster:q(c2, ["GET","nonexists"])),
            ?assertEqual({ok, undefined}, eredis_cluster:q(c3, ["GET","nonexists"]))
        end
        },

        { "multi node get",
            fun () ->
                N=1000,
                Keys = [integer_to_list(I) || I <- lists:seq(1,N)],
                [eredis_cluster:q(c1, ["SETEX", Key, "50", Key ++ "c1"]) || Key <- Keys],
                [eredis_cluster:q(c2, ["SETEX", Key, "50", Key ++ "c2"]) || Key <- Keys],
                [eredis_cluster:q(c3, ["SETEX", Key, "50", Key ++ "c3"]) || Key <- Keys],

                Guard11 = [{ok, list_to_binary(Key ++ "c1")} || Key <- Keys],
                ?assertMatch(Guard11, eredis_cluster:qmn(c1, [["GET", Key] || Key <- Keys])),

                Guard12 = [{ok, list_to_binary(Key ++ "c2")} || Key <- Keys],
                ?assertMatch(Guard12, eredis_cluster:qmn(c2, [["GET", Key] || Key <- Keys])),

                Guard13 = [{ok, list_to_binary(Key ++ "c3")} || Key <- Keys],
                ?assertMatch(Guard13, eredis_cluster:qmn(c3, [["GET", Key] || Key <- Keys])),

                eredis_cluster:q(c1, ["SETEX", "a", "50", "1"]),
                eredis_cluster:q(c2, ["SETEX", "a", "50", "2"]),
                eredis_cluster:q(c3, ["SETEX", "a", "50", "3"]),

                Guard21 = [{ok, integer_to_binary(1)} || _Key <- lists:seq(1,5)],
                ?assertMatch(Guard21, eredis_cluster:qmn(c1, [["GET", "a"] || _I <- lists:seq(1,5)])),

                Guard22 = [{ok, integer_to_binary(2)} || _Key <- lists:seq(1,5)],
                ?assertMatch(Guard22, eredis_cluster:qmn(c2, [["GET", "a"] || _I <- lists:seq(1,5)])),

                Guard23 = [{ok, integer_to_binary(3)} || _Key <- lists:seq(1,5)],
                ?assertMatch(Guard23, eredis_cluster:qmn(c3, [["GET", "a"] || _I <- lists:seq(1,5)]))
            end
        },

        { "flushdb",
        fun () ->
            eredis_cluster:q(c1, ["set", "zyx", "test"]),
            eredis_cluster:q(c2, ["set", "zyxw", "test"]),
            eredis_cluster:q(c3, ["set", "zyxwv", "test"]),
            eredis_cluster:flushdb(c1),
            eredis_cluster:flushdb(c2),
            eredis_cluster:flushdb(c3),
            ?assertEqual({ok, undefined}, eredis_cluster:q(c1, ["GET", "zyx"])),
            ?assertEqual({ok, undefined}, eredis_cluster:q(c2, ["GET", "zyxw"])),
            ?assertEqual({ok, undefined}, eredis_cluster:q(c3, ["GET", "zyxwv"]))
        end
        }
    ]}
}.
