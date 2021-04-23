%%%-------------------------------------------------------------------
%%% @author manlin
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 4月 2021 21:24
%%%-------------------------------------------------------------------
-module(lib).
-author("manlin").

%% API
-export([
	binary_to_hex/1,
	now/0,
	four_clock_sec/0,
	sub_list/2
]).

binary_to_hex(Bin)->
	<< <<(hex(A))>> || <<A:4>> <= Bin >>.

hex(X)when X =< 9->
	$0+X;
hex(X)->
	$a+X-10.

now()->
	erlang:system_time(second).

%% 距离第二天午夜4点多少秒
four_clock_sec()->
	{H,M,S} = T = erlang:time(),
	case T >= {4,0,0} of
		true->
			86400-(H*3600+M*60+S-4*3600);
		false->
			4*3600-(H*3600+M*60+S)
	end.

sub_list(L,Len)->
	sub_list(L,Len,[]).

sub_list([],_,Acc)->
	{lists:reverse(Acc),[]};
sub_list(Less,0,Acc)->
	{lists:reverse(Acc),Less};
sub_list([H|T],Len,Acc)->
	sub_list(T,Len-1,[H|Acc]).

