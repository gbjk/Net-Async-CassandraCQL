#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Identity;

use Net::Async::CassandraCQL;
use Protocol::CassandraCQL::Result 0.06;

# Mock the ->_connect_node method
no warnings 'redefine';
my %conns;
local *Net::Async::CassandraCQL::_connect_node = sub {
   my $self = shift;
   my ( $connect_host, $connect_service ) = @_;
   $conns{$connect_host} = my $conn = TestConnection->new;
   $conn->{nodeid} = $connect_host;
   return Future->new->done( $conn );
};

my $cass = Net::Async::CassandraCQL->new(
   host => "10.0.0.1",
);

my $f = $cass->connect;

ok( defined $conns{"10.0.0.1"}, 'have a connection to 10.0.0.1' );

my @pending_queries;

# Initial nodelist query
while( @pending_queries ) {
   my $q = shift @pending_queries;
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
               [ "\x0a\0\0\3", "DC1", "rack1" ],
            ],
         ),
      );
   }
   else {
      fail( "Unexpected initial query $q->[1]" );
   }
}

# Fake closure
undef $conns{"10.0.0.1"};
$cass->_closed_node( "10.0.0.1" );

ok( defined $conns{"10.0.0.2"} || defined $conns{"10.0.0.3"},
    'new primary node picked' );

# ->query after reconnect
{
   my $f = $cass->query( "GET THING", 0 );
   ok( defined $f, 'defined ->query after reconnect' );

   ok( scalar @pending_queries, '->query after reconnect creates query' );

   my $q = shift @pending_queries;
   like( $q->[0], qr/^10\.0\.0\.[23]$/, '$q conn' );
   is( $q->[1], "GET THING", '$q cql' );
   $q->[2]->done( result => "here" );

   is_deeply( [ $f->get ], [ result => "here" ], '$q result' );
}

done_testing;

package TestConnection;
use base qw( Net::Async::CassandraCQL::Connection );

sub query
{
   my $self = shift;
   my ( $cql ) = @_;
   push @pending_queries, [ $self->nodeid, $cql, my $f = Future->new ];
   return $f;
}
