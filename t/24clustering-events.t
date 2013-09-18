#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Identity;
use Test::Refcount;

use t::MockConnection;
use Socket qw( pack_sockaddr_in inet_aton );

use Net::Async::CassandraCQL;
use Protocol::CassandraCQL::Result 0.06;

# Mock the ->_connect_node method
no warnings 'redefine';
my %conns;
local *Net::Async::CassandraCQL::_connect_node = sub {
   my $self = shift;
   my ( $connect_host, $connect_service ) = @_;
   $conns{$connect_host} = my $conn = t::MockConnection->new( $connect_host );
   return Future->new->done( $conn );
};

my $cass = Net::Async::CassandraCQL->new(
   host => "10.0.0.1",
);

my $f = $cass->connect;

ok( my $c = $conns{"10.0.0.1"}, 'Connected to 10.0.0.1' );

# Initial nodelist query
while( my $q = $c->next_query ) {
   if( $q->[1] eq "SELECT data_center, rack FROM system.local" ) {
      pass( "Query on system.local" );
      $q->[2]->done( rows =>
         Protocol::CassandraCQL::Result->new(
            columns => [
               [ system => local => data_center => "VARCHAR" ],
               [ system => local => rack        => "VARCHAR" ],
            ],
            rows => [
               [ "DC1", "rack1" ],
            ],
         )
      );
   }
   elsif( $q->[1] eq "SELECT peer, data_center, rack FROM system.peers" ) {
      pass( "Query on system.peers" );
      $q->[2]->done( rows =>
         Protocol::CassandraCQL::Result->new(
            columns => [
               [ system => peers => peer        => "VARCHAR" ],
               [ system => peers => data_center => "VARCHAR" ],
               [ system => peers => rack        => "VARCHAR" ],
            ],
            rows => [
               [ "\x0a\0\0\2", "DC1", "rack1" ],
            ],
         ),
      );
   }
   else {
      fail( "Unexpected initial query $q->[1]" );
   }
}

$f->get;

ok( $c->is_registered, 'Using 10.0.0.1 for events' );

ok( !defined $cass->{nodes}{"10.0.0.2"}{down_time}, 'Node 10.0.0.2 does not yet have down_time' );

$conns{"10.0.0.1"}->invoke_event(
   on_status_change => DOWN => pack_sockaddr_in( 0, inet_aton( "10.0.0.2" ) ),
);

ok( defined $cass->{nodes}{"10.0.0.2"}{down_time}, 'Node 10.0.0.2 has down_time after STATUS_CHANGE DOWN' );

done_testing;
