#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Identity;

use Net::Async::CassandraCQL;

# Mock the ->_connect_node method
no warnings 'redefine';
my @connect_futures;
my ( $connect_host, $connect_service );
local *Net::Async::CassandraCQL::_connect_node = sub {
   shift;
   die "No connect future pending" unless @connect_futures;
   ( $connect_host, $connect_service ) = @_;
   return shift @connect_futures;
};

my $cass = Net::Async::CassandraCQL->new(
   host => "my-seed",
);

push @connect_futures, my $conn_f = Future->new;

my $f = $cass->connect;

ok( defined $f, 'defined $f for ->connect' );
is( $connect_host,    "my-seed", '->connect host' );
is( $connect_service, 9042,      '->connect service' );
ok( !$f->is_ready, '$f not yet ready' );

my $conn = TestConnection->new;
$cass->add_child( $conn );
$conn_f->done( $conn );

ok( $f->is_ready, '$f is now ready' );

# ->query
my @pending_queries;
{
   $f = $cass->query( "DO SOMETHING now", 0 );

   ok( scalar @pending_queries, '@pending_queries after ->query' );

   identical( $pending_queries[0][0], $conn, 'connection on pending query' );
   is( $pending_queries[0][1], "DO SOMETHING now", 'cql for pending query' );

   ( shift @pending_queries )->[2]->done;
   ok( $f->is_ready, '$f is now ready' );
}

done_testing;

package TestConnection;
use base qw( Net::Async::CassandraCQL::Connection );

sub query
{
   my $self = shift;
   my ( $cql ) = @_;
   push @pending_queries, [ $self, $cql, my $f = Future->new ];
   return $f;
}
