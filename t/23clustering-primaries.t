#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Identity;
use Test::Refcount;

use Net::Async::CassandraCQL;
use Protocol::CassandraCQL::Result 0.06;

# Mock the ->_connect_node method
no warnings 'redefine';
my %conns;
my %conn_f;
local *Net::Async::CassandraCQL::_connect_node = sub {
   my $self = shift;
   my ( $connect_host, $connect_service ) = @_;
   $conns{$connect_host} = my $conn = TestConnection->new;
   $conn->{nodeid} = $connect_host;
   return $conn_f{$connect_host} = Future->new;
};

my $cass = Net::Async::CassandraCQL->new(
   host => "10.0.0.1",
   primaries => 3,
);

my $f = $cass->connect;

$conn_f{"10.0.0.1"}->done( $conns{"10.0.0.1"} );

my @pending_queries;
my @pending_prepares;

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

is( scalar keys %conns, 3, 'All three server connect attempts after ->connect' );

# complete one but not the other
$conn_f{"10.0.0.2"}->done( $conns{"10.0.0.2"} );

# Queries should RR between the two currently connected
{
   my @f = map { $cass->query( "GET THING", 0 ) } 1 .. 6;

   is( scalar @pending_queries, 6, '6 pending queries' );
   my %q_by_nodeid;
   while( my $q = shift @pending_queries ) {
      $q_by_nodeid{$q->[0]}++;
      $q->[2]->done( result => "here" );
   }

   is_deeply( \%q_by_nodeid,
              { "10.0.0.1" => 3,
                "10.0.0.2" => 3 },
              'Queries distributed per ready node' );

   Future->needs_all( @f )->get;
}

# now complete the final
$conn_f{"10.0.0.3"}->done( $conns{"10.0.0.3"} );

{
   my @f = map { $cass->query( "GET THING", 0 ) } 1 .. 6;

   is( scalar @pending_queries, 6, '6 pending queries' );
   my %q_by_nodeid;
   while( my $q = shift @pending_queries ) {
      $q_by_nodeid{$q->[0]}++;
      $q->[2]->done( result => "here" );
   }

   is_deeply( \%q_by_nodeid,
              { "10.0.0.1" => 2,
                "10.0.0.2" => 2,
                "10.0.0.3" => 2 },
              'Queries distributed per node' );

   Future->needs_all( @f )->get;
}

$f->get;

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

sub prepare
{
   my $self = shift;
   my ( $cql ) = @_;
   push @pending_prepares, [ $self->nodeid, $cql, my $f = Future->new ];
   return $f;
}

sub register
{
   return Future->new->done;
}
