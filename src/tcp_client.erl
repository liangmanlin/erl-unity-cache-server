%%%----------------------------------------------------------------------------------
%%% @author manlin
%%% @doc
%%%
%%% @end
%%% Create : 2021-04-13. 19:20
%%%----------------------------------------------------------------------------------
-module(tcp_client).
-author("manlin").
-include_lib("kernel/include/file.hrl").

-behaviour(gen_server).

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-export([start_link/1]).

-record(state, {}).

start_link(Socket) ->
	gen_server:start_link(?MODULE, [Socket], []).

init([Socket]) ->
	rand:seed(exrop),
	put(socket, Socket),
	{ok, #state{}}.

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast({connect}, State) ->
	Socket = get_socket(),
	{ok, Bin} = gen_tcp:recv(Socket, 0, 3000),
	Size = erlang:byte_size(Bin),
	case inet:peername(Socket) of
		{ok, {IP, _}} ->
			io:format("~w connect version:~ts~n", [IP, Bin]);
		_ ->
			ok
	end,
	case Size < 8 of
		true ->
			BinSend = list_to_binary([lists:duplicate(8 - Size, $0), Bin]),
			gen_tcp:send(Socket, BinSend);
		false ->
			gen_tcp:send(Socket, Bin)
	end,
	inet:setopts(Socket, [{active, true}]),
	{noreply, State};

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info({tcp, _Socket, Bin}, State) ->
	read_bin(Bin),
	{noreply, State};
handle_info({tcp_closed, Socket}, State) ->
	io:format("cache client exit~n"),
	case get_socket() of
		Socket ->
			erase(socket),
			catch erlang:port_close(Socket),
			{stop, normal, State};
		_ ->
			{noreply, State}
	end;
handle_info({tcp_error, Socket, Reason}, State) ->
	io:format("cache client exit:~w~n", [Reason]),
	case get_socket() of
		Socket ->
			erase(socket),
			catch erlang:port_close(Socket),
			{stop, normal, State};
		_ ->
			{noreply, State}
	end;
handle_info(closed, State) ->
	{stop, normal, State};
handle_info(Info, State) ->
	io:format("unknow info:~w~n", [Info]),
	{noreply, State}.

terminate(_Reason, _State) ->
	case get_socket() of
		undefined -> ok;
		Socket ->
			erase(socket),
			catch erlang:port_close(Socket)
	end,
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

read_bin(Bin) ->
	case get(rec_bin_count) of
		undefiend ->
			match_op(Bin);
		Count ->
			ByteSize = erlang:byte_size(Bin),
			case ByteSize < Count of
				true ->
					put(rec_bin_count, Count - ByteSize),
					add_op_bin(Bin);
				false ->
					erase(rec_bin_count),
					<<Bin2:Count/binary, Bin3/binary>> = Bin,
					act_op(Count, Bin2),
					read_bin(Bin3)
			end
	end.

match_op(<<>>) ->
	ok;
%%????????????????????????cache???????????????cache?????????
match_op(<<$g, O:8, Bin/binary>>) ->
	ByteSize = erlang:byte_size(Bin),
	case ByteSize < 32 of
		true ->
			put(rec_bin_count, 32 - ByteSize),
			put(op, {get, O, [Bin]});
		false ->
			erase(rec_bin_count),
			erase(op),
			<<Key:32/binary, Bin2/binary>> = Bin,
			get_file(O, Key),
			match_op(Bin2)
	end;
%% ????????????????????????????????????
match_op(<<"ts", Bin/binary>>) ->
	ByteSize = erlang:byte_size(Bin),
	case ByteSize < 32 of
		true ->
			put(rec_bin_count, 32 - ByteSize),
			put(op, {get, 0, [Bin]});
		false ->
			<<Key:32/binary, Bin2/binary>> = Bin,
			start_push_file(Key),
			match_op(Bin2)
	end;
%% ??????????????????????????????????????????????????????ts??????
match_op(<<$p, I, Bin/binary>>) when I == $a orelse I == $i orelse I == $r ->
	ByteSize = erlang:byte_size(Bin),
	case ByteSize < 16 of
		true ->
			put(rec_bin_count, 16 - ByteSize),
			put(op, {get, I, [Bin]});
		false ->
			<<LenStr:16/binary, Bin2/binary>> = Bin,
			Size = erlang:binary_to_integer(LenStr, 16),
			io:format("p~s size :~w~n", [[I], Size]),
			put(rec_bin_count, Size),
			put(op, {file_bin, I, []}),
			read_bin(Bin2)
	end;
%% ??????????????????????????????
match_op(<<"te", Bin/binary>>) ->
	end_push_file(),
	match_op(Bin);
%% ????????????
match_op(<<"q", _/binary>>) ->
	io:format("cache client exit~n"),
	erlang:send(self(), closed);

match_op(Bin) ->
	case erase(cache_bin) of
		undefined ->
			put(cache_bin, Bin);
		B ->
			Bin2 = <<B/binary, Bin/binary>>,
			match_op(Bin2)
	end.

add_op_bin(Bin) ->
	{OP, O, BinTotal} = get(op),
	put(op, {OP, O, [Bin | BinTotal]}).

act_op(Count, Bin) ->
	{OP, O, BinTotal} = erase(op),
	case OP of
		get ->
			BinTotal2 = list_to_binary(lists:reverse([Bin | BinTotal])),
			get_file(O, BinTotal2);
		push ->
			BinTotal2 = list_to_binary(lists:reverse([Bin | BinTotal])),
			start_push_file(BinTotal2);
		p ->
			BinTotal2 = list_to_binary(lists:reverse([Bin | BinTotal])),
			Size = erlang:binary_to_integer(BinTotal2, 16),
			put(rec_bin_count, Size),
			put(op, {file_bin, O, []});
		file_bin ->
			write_file(Count, O, [Bin | BinTotal])
	end.

get_file(O, Key) ->
	<<UID:16/binary, _/binary>> = Key,
	Ets = unity_cache_server:get_ets(UID),
	case ets:lookup(Ets, {Key, O}) of
		[] ->
			send_pack(<<$-, O:8, Key/binary>>);
		[{_, Path}] ->
			FileName = unity_cache_server:get_file_name(Path, Key, O),
			send_file(O, Key, FileName)
	end.

send_file(O, Key, FileName) ->
	case file:read_file_info(FileName) of
		{ok, FileInfo} ->
			#file_info{size = Size} = FileInfo,
			%% ??????????????????
			LenStr = list_to_binary(io_lib:format("~16.16.0b", [Size])),
			send_pack(<<$+, O:8, LenStr/binary, Key/binary>>),
			%% ???2M???????????????????????????
			{ok, FD} = prim_file:open(FileName, [read, raw, binary]),
			send_file_2(Size, FD);
		_ ->
			send_pack(<<$-, O:8, Key/binary>>)
	end.

send_file_2(Size, FD) when Size >= 2097152 ->
	case prim_file:read(FD, 2097152) of
		eof ->
			prim_file:close(FD),
			ok;
		{ok, Bin} ->
			send_pack(Bin),
			send_file_2(Size, FD)
	end;
send_file_2(Size, FD) ->
	{ok, Bin} = prim_file:read(FD, Size),
	send_pack(Bin),
	ok.

send_pack(Bin) ->
	catch gen_tcp:send(get_socket(), Bin).

start_push_file(Key) ->
	io:format("start putting ~s~n", [lib:binary_to_hex(Key)]),
	%% ?????????????????????????????????????????????????????????????????????????????????????????????????????????
	case gen_server:call(unity_cache_server, {start_check, Key, self()}) of
		true ->
			Path = unity_cache_server:get_random_path(),
			FileName = unity_cache_server:get_file_name(Path, Key, false),
			put(file_io, {key, Path, FileName, []});
		false ->
			ok
	end.

write_file(_TotalSize, O, BinList) ->
	case get(file_io) of
		undefined -> ok;
		{Key, Path, FileName, L} ->
			FileName2 = FileName ++ [$., O],
			put(file_io, {Key, Path, FileName, [O | lists:delete(O, L)]}),
			file:write_file(FileName2, lists:reverse(BinList), [append])
	end.

end_push_file() ->
	case erase(file_io) of
		undefined -> ok;
		{Key, Path, _FileName, L} ->
			<<UID:16/binary, Hash/binary>> = Key,
			Index = unity_cache_server:hash_key(UID),
			Ets = unity_cache_server:get_ets(Index),
			Ets2 = unity_cache_server:get_uid_ets(Index),
			lists:foreach(
				fun(O) ->
					case ets:lookup(Ets2, {UID, O}) of
						[] ->
							ets:insert(Ets2, {{UID, O}, [{Hash, lib:now()}]});
						[{K, LL}] ->
							L2 = lists:keystore(Hash, 1, LL, {Hash, lib:now()}),
							ets:insert(Ets2, {K, L2})
					end,
					ets:insert(Ets, {{Key, O}, Path})
				end, L),
			gen_server:cast(unity_cache_server, {end_push_file, Key, Index, self()})
	end.

get_socket()->
	get(socket).

