#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::HexString;

use IO::Async::Test;
use IO::Async::OS;
use IO::Async::Loop;
use IO::Async::Stream;

use Net::Async::CassandraCQL;
use Protocol::CassandraCQL qw( CONSISTENCY_ANY CONSISTENCY_ONE );

my $loop = IO::Async::Loop->new();
testing_loop( $loop );

my ( $S1, $S2 ) = IO::Async::OS->socketpair() or die "Cannot create socket pair - $!";

my $cass = Net::Async::CassandraCQL->new(
   transport => IO::Async::Stream->new( handle => $S1 )
);

$loop->add( $cass );

# void
{
   my $f = $cass->query( "INSERT INTO things (name) VALUES ('thing');", CONSISTENCY_ANY );

   my $stream = "";
   wait_for_stream { length $stream >= 8 + 49 } $S2 => $stream;

   # OPCODE_QUERY
   is_hexstr( $stream,
              "\x01\x00\x01\x07\0\0\0\x31" .
                 "\0\0\0\x2bINSERT INTO things (name) VALUES ('thing');\0\0",
              'stream after ->query INSERT' );

   # OPCODE_RESULT
   $S2->syswrite( "\x81\x00\x01\x08\0\0\0\4\0\0\0\1" );

   wait_for { $f->is_ready };

   is_deeply( [ $f->get ], [],
              '->query returns nothing' );
}

# rows
{
   my $f = $cass->query( "SELECT a,b FROM c;", CONSISTENCY_ONE );

   my $stream = "";
   wait_for_stream { length $stream >= 8 } $S2 => $stream;

   # OPCODE_QUERY
   is_hexstr( $stream,
              "\x01\x00\x01\x07\0\0\0\x18" .
                 "\0\0\0\x12SELECT a,b FROM c;\0\1",
              'stream after ->query SELECT' );

   # OPCODE_RESULT
   $S2->syswrite( "\x81\x00\x01\x08\0\0\0\x34\0\0\0\2" .
                     "\0\0\0\1\0\0\0\2\0\4test\0\1c\0\1a\x00\x0D\0\1b\x00\x09" . # metadata
                     "\0\0\0\1" . # row count
                     "\0\0\0\5hello\0\0\0\4\0\0\0\x64" # row 0
                  );

   wait_for { $f->is_ready };

   is( scalar $f->get, "rows", '->query SELECT returns rows' );

   my $result = ( $f->get )[1];
}

# set_keyspace
{
   my $f = $cass->query( "USE test;", CONSISTENCY_ANY );

   my $stream = "";
   wait_for_stream { length $stream >= 8 + 13 } $S2 => $stream;

   # OPCODE_QUERY
   is_hexstr( $stream,
              "\x01\x00\x01\x07\0\0\0\x0f" .
                 "\0\0\0\x09USE test;\0\0",
              'stream after ->query USE' );

   # OPCODE_RESULT
   $S2->syswrite( "\x81\x00\x01\x08\0\0\0\x0a\0\0\0\3\0\4test" );

   wait_for { $f->is_ready };

   is_deeply( [ $f->get ], [ keyspace => "test" ],
              '->query USE returns keyspace' );
}

# schema_change
{
   my $f = $cass->query( "DROP TABLE users;", CONSISTENCY_ANY );

   my $stream = "";
   wait_for_stream { length $stream >= 8 + 21 } $S2 => $stream;

   # OPCODE_QUERY
   is_hexstr( $stream,
              "\x01\x00\x01\x07\0\0\0\x17" .
                 "\0\0\0\x11DROP TABLE users;\0\0",
              'stream after ->query DROP TABLE' );

   # OPCODE_RESULT
   $S2->syswrite( "\x81\x00\x01\x08\0\0\0\x1a\0\0\0\5\0\7DROPPED\0\4test\0\5users" );

   wait_for { $f->is_ready };

   is_deeply( [ $f->get ], [ schema_change => [qw( DROPPED test users )] ],
              '->query DROP TABLE returns schema change' );
}

done_testing;
