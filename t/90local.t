#!/usr/bin/perl

use strict;
use warnings;

my %CONFIG;
use Test::More;

BEGIN {
   # This test attempts to talk to a real Cassandra database, and will
   # create its own keyspace to work in.
   #
   # It is disabled by default, but to enable it create a t/local.yaml file
   # containing the host, keyspace, and optionally service port number to
   # connect to.
   -e "t/local.yaml" or plan skip_all => "No t/local.yaml config";
   require YAML;
   %CONFIG = %{ YAML::LoadFile( "t/local.yaml" ) };
   defined $CONFIG{host} or plan skip_all => "t/local.yaml does not define a host";
   defined $CONFIG{keyspace} or plan skip_all => "t/local.yaml does not define a keyspace";
}

use Test::HexString;

use IO::Async::Test;
use IO::Async::Loop;

use Net::Async::CassandraCQL;
use Protocol::CassandraCQL qw( CONSISTENCY_ONE );

my $loop = IO::Async::Loop->new();
testing_loop( $loop );

my $cass = Net::Async::CassandraCQL->new;
$loop->add( $cass );

$cass->connect( host => $CONFIG{host}, service => $CONFIG{port} )->get;

$cass->query( "USE $CONFIG{keyspace};", CONSISTENCY_ONE )->get;

$cass->query( "CREATE TABLE tbl1 (key varchar PRIMARY KEY, c1 varchar);", CONSISTENCY_ONE )->get;
my $table = 1;
END { $table and $cass->query( "DROP TABLE tbl1;", CONSISTENCY_ONE )->await }

pass( "CREATE TABLE" );

$cass->query( "INSERT INTO tbl1 (key, c1) VALUES ('the-key', 'the-value');", CONSISTENCY_ONE )->get;
pass( "INSERT INTO tbl" );

my ( $type, $result ) = $cass->query( "SELECT key, c1 FROM tbl1;", CONSISTENCY_ONE )->get;

is( $type, "rows", "SELECT query result type is rows" );

is( $result->columns, 2, '$result has 2 columns' );
is( $result->rows, 1, '$result has 1 row' );
is( $result->column_type(0), "VARCHAR", 'column_type 0' );
is( $result->column_type(1), "VARCHAR", 'column_type 1' );

is_deeply( [ $result->rowbytes(0) ],
           [ "the-key", "the-value" ],
           'rowbytes(0)' );

done_testing;
