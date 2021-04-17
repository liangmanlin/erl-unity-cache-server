  
%%%----------------------------------------------------------------------------------
%%% @author manlin
%%% @doc
%%%
%%% @end
%%% Create : 2021-04-17. 9:20
%%%----------------------------------------------------------------------------------
-module(unity_cache_server).

-define(DAY_SECONDS,86400).

-behaviour(gen_server).

-export([start_link/0,start/0,get_random_path/0,get_file_name/3,ctl/1,get_ets/1,get_uid_ets/1,hash_key/1]).

-export([init/1,handle_call/3,handle_cast/2,handle_info/2,terminate/2,code_change/3]).

-define(DEFAULT_ROOT,"/data/unity_cache").

-define(dump_time,60).

-define(CACHE_FILE_SIZE,20).

-record(state,{}).

start()->
    io:formt("~s~n",["startint server"]),
    start_svr(),
    gate_app:listen(cache_server,get_port(),[binary,{reuseaddr,true}],tcp_client),
    io:format("~s~n",["start done"]).

ctl([Node,stop])->
    rpc:call(Node,init,stop,[]),
    halt(0).

start_svr()->
    {ok,_} = supervisor:start_child(gate_sup,#{id=>?MODULE,restart=>transient,shutdown=>30000,type=>worker,start=>{?MODULE,start_linke,[]},modules=>[?MODULE]}).

start_link()->
    gen_server:start_link({local,?MODULE},?MODULE,[],[]).

init([])->
    erlang:process_flag(trap_exit,true),
    Root = get_root(),
    build_path(Root),
    load_cache_file(Root),
    try_build_ets(),
    erlang:send_after(?dump_time*1000,self(),loop),
    %% 每天4点清理
    T = four_clock_sec(),
    erlang:send_after(T*1000,self(),clean),
    {ok,#state{}}.

handle_call({start_check,Key,Pid},_From,State)->
    Reply =
        case get({push_file,Key}) of
            undefined->
                put({push_file,Key},Pid),
                Ref = erlang:monitor(process,Pid),
                put(Pid,{key,Ref}),
                true;
            _->
                false
        end,
    {reply,Reply,State};
handle_call(_Request,_From,State)->
    {reply,ok,State}.

handle_cast({end_push_file,Key,Hash,Pid},State)->
    erase({push_file,Key}),
    case erase(Pid) of
        {_,Ref}->
            erlang:demonitor(Ref);
        _->
            ok
    end,
    put({dirty,Hash},true),
    {noreply,State};
handle_cast(_Request,State)->
    {noreply,State}.

handle_info({'DOWN',_Ref,_,Pid,_Reason},State)->
    case erase(Pid) of
        {Key,_}->
            erase({push_file,Key});
        _->
            ok
    end,
    {noreply,State};
handle_info(loop,State)->
    erlang:send_after(?dump_time*1000,self(),loop),
    lists:foreach(
                  fun(I)->
                      case erase({dirty,I}) of
                          true->
                              erlang:spawn(fun()->dump(I) end);
                           _->
                               ok
                       end
                  end,lists:seq(1,?CACHE_FILE_SIZE)),
   {noreply,State};
handle_info(clean,State)->
    erlang:send_after(?DAY_SECONDS*1000,self(),clean),
    Level = get_level(),
    lists:foreach(
                  fun(I)->
                      do_clean(Level,I)
                            end,lists:seq(1,?CACHE_FILE_SIZE)),
    {noreply,State};
handle_info({clean,Level},State)->
    lists:foreach(
                  fun(I)->
                      do_clean(Level,I)
                            end,lists:seq(1,?CACHE_FILE_SIZE)),
    {noreply,State};
handle_info(_Info,State)->
    {noreply,State}.

terminate(_Reason,_State)->
    lists:foreach(
                  fun(I)->
                      case erase({dirty,I}) of
                          true->
                              dump(I);
                          _->
                              ok
                      end
                      end,lists:seq(1,?CACHE_FILE_SIZE)),
    ok.

code_change(_OldVsn,State,_Extra)->
    {ok,State}.

dump(Index)->
    Root = get_root(),
    Ets1 = get_ets(Index),
    Ets2 = get_uid_ets(Index),
    ets:tab2file(Ets1,Root++"/file_cache_ets_"++integer_to_list(Index)++".ets",
    ets:tab2file(Ets1,Root++"/uid_ets_"++integer_to_list(Index)++".ets",
    ok.

get_ets(Index) when is_integer(Index)->
    list_to_atom("file_cache_ets_"++integer_to_list(Index));
get_ets(UID)->
    get_ets(erlang:phash(UID,?CACHE_FILE_SIZE)).

hash_key(UID)->
    erlang:phash(UID,?CACHE_FILE_SIZE).

-define(path_1_num,255).
-define(path_2_num,255).

get_random_path()->
    Path1 = rand:uniform(9999999) rem ?path_1_num,
    Path2 = rand:uniform(9999999) rem ?path_2_num,
    {Path1,Path2}.

get_file_name(Path,Key,O)->
    {Path1,Path2} = Path,
    Root = get_root(),
    Name = binary_to_hex(Key),
    case O of
        false->
            io_lib:format("~s/~p/~p/~s",[Root,Path1,Path2,Name]);
        _->
           io_lib:format("~s/~p/~p/~s.~s",[Root,Path1,Path2,Name,[O]])
    end.

build_path(Root)->
    os:cmd(mkdir -p "++Root),
    {ok,RootL} = prim_file:list_dir(Root),
    RM = maps:from_list([{P,true}||P<-RootL]),
    lists:foreach(
        fun(I)->
            Path1 = io_lib:format("~s/~p",[Root,I]),
            case maps:is_key(integer_to_list(I),RM) of
                true->ok;
                false->prim_file:make_dir(Path1)
            end,
            {ok,L} = prim_file:list_dir(Path1),
            M = maps:from_list([{P,true}||P<-L]),
            lists:foreach(
                fun(J)->
                    case maps:is_key(integer_to_list(J),M) of
                        true->ok;
                        false->
                            Path2 = io_lib:format("~s/~p",[Path1,J]),
                            prim_file:make_dir(Path2)
                    end
                end,lists:seq(0,?path_2_num-1))
  end,lists:seq(0,?path_1_num-1)).
  
get_root()->
    case init:get_argument(root_path) of
        {ok,[[Root]]}->
            Root;
         _->
            ?DEFAULT_ROOT
    end.
    
 get_port()->
    case init:get_argument(port) of
        {ok,[[Port]]}->
            list_to_integer(Port);
        _->
            20014
    end.
    
 get_level()->
    1.
    
 do_clean(_Level,Index)->
    Ets1 = get_ets(Index),
    Ets2 = get_uid_ets(Index),
    CheckTime = now()-5*?DAY_SECONDS,
    Rec = 1,
    DelList =
    ets:foldl(
        fun({Key,List},Acc)->
            case List of
                []->Acc;
                [_] when Rec > 0 ->Acc;
                _->
                    case Rec > 0 of
                        true->
                            SL = lists:sort(fun({_,A},{_,B})->A>B end,List),
                            {List2,Less} = sub_list(SL,Rec);
                        false->
                            List2 = [],Less = List
                    end,
                    case [A||{_,Time}=A<-Less,CheckTime >= Time] of
                        []->
                            Acc;
                        Del->
                            Less2 = Less--Del,
                            ets:update_element(Ets2,Key,{2,List2++Less2}),
                            [{Key,Del}|Acc]
                    end
             end
      end,[],Ets2),
    lists:foreach(
        fun({{UID,O},DL})->
            lists:foreach(
                fun({Hash,_})->
                    Key = <<UID/binary,Hash/binary>>,
                    case ets:lookup(Ets1,{Key,O}) of
                        []->ok;
                        [{_,Path}]->
                            FileName = get_file_name(Path,Key,O),
                            io:format("del ~s~n",[FileName]),
                            file:delete(FileName)
                    end,
                    ets:delete(Ets1,{Key,O})
                end,DL)
          end,DelList),
  case DelList of
    []->ok;
    _->
        put({dirty,Index},true)
  end.
  
load_cache_file(Root)->
    lists:foreach(
        fun(Hash)->
            ets:file2tab(Root++"/file_cache_ets_"++integer_to_list(Hash)++".ets"),
            ets:file2tab(Root++"/uid_ets_"++integer_to_list(Hash)++".ets")
        end,lists:seq(1,?CACHE_FILE_SIZE)).
        

