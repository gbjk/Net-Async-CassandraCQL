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

$cass->query( "CREATE TABLE tbl1 (key varchar PRIMARY KEY, t1 varchar, i1 int);", CONSISTENCY_ONE )->get;
my $table = 1;
END { $table and $cass->query( "DROP TABLE tbl1;", CONSISTENCY_ONE )->await }

pass( "CREATE TABLE" );

$cass->query( "INSERT INTO tbl1 (key, t1) VALUES ('the-key', 'the-value');", CONSISTENCY_ONE )->get;
pass( "INSERT INTO tbl" );

# SELECT as ->query
{
   my ( $type, $result ) = $cass->query( "SELECT key, t1 FROM tbl1;", CONSISTENCY_ONE )->get;

   is( $type, "rows", "SELECT query result type is rows" );

   is( $result->columns, 2, '$result has 2 columns' );
   is( $result->rows, 1, '$result has 1 row' );
   is( scalar $result->column_name(0), "$CONFIG{keyspace}.tbl1.key", 'column_name 0' );
   is( scalar $result->column_name(1), "$CONFIG{keyspace}.tbl1.t1",  'column_name 1' );
   is( $result->column_type(0), "VARCHAR", 'column_type 0' );
   is( $result->column_type(1), "VARCHAR", 'column_type 1' );

   is_deeply( $result->row_array(0),
              [ "the-key", "the-value" ],
              'row_array(0)' );

   is_deeply( $result->row_hash(0),
              { key => "the-key", t1 => "the-value" },
              'row_hash(0)' );
}

# INSERT as ->prepare / ->execute
{
   my ( $id, $meta ) = $cass->prepare( "INSERT INTO tbl1 (key, i1) VALUES (?, ?);" )->get;

   ok( length $id, '$id is set for prepared INSERT' );

   is( $meta->columns, 2, '$meta has 2 columns for prepared INSERT' );
   is( scalar $meta->column_name(0), "$CONFIG{keyspace}.tbl1.key", 'column_name 0' );
   is( scalar $meta->column_name(1), "$CONFIG{keyspace}.tbl1.i1",  'column_name 1' );
   is( $meta->column_type(0), "VARCHAR", 'column_type 0' );
   is( $meta->column_type(1), "INT",     'column_type 1' );

   $cass->execute( $id, [ $meta->encode_data( "another-key", 123456789 ) ], CONSISTENCY_ONE )->get;
}

# SELECT as ->prepare / ->execute
{
   my ( $id, $meta ) = $cass->prepare( "SELECT i1 FROM tbl1 WHERE key = ?;" )->get;

   ok( length $id, '$id is set for prepared SELECT' );

   is( $meta->columns, 1, '$meta has 1 column for prepared SELECT' );

   my ( $type, $result ) = $cass->execute( $id, [ $meta->encode_data( "another-key" ) ], CONSISTENCY_ONE )->get;

   is( $type, "rows", 'SELECT prepare/execute result type is rows' );

   is_deeply( $result->row_array(0),
              [ 123456789 ],
              'row_array(0) for SELECT prepare/execute' );
}

done_testing;
