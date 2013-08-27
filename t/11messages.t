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

my $loop = IO::Async::Loop->new();
testing_loop( $loop );

my ( $S1, $S2 ) = IO::Async::OS->socketpair() or die "Cannot create socket pair - $!";

my $cass = Net::Async::CassandraCQL->new(
   transport => IO::Async::Stream->new( handle => $S1 )
);

$loop->add( $cass );

{
   my $f = $cass->startup;

   my $stream = "";
   wait_for_stream { length $stream >= 8 + 22 } $S2 => $stream;

   # OPCODE_STARTUP
   is_hexstr( $stream,
              "\x01\x00\x01\x01\0\0\0\x16" .
                 "\x00\x01" . "\x00\x0bCQL_VERSION\x00\x053.0.0",
              'stream after ->startup' );

   # OPCODE_READY
   $S2->syswrite( "\x81\x00\x01\x02\0\0\0\0" );

   wait_for { $f->is_ready };

   is_deeply( [ $f->get ], [],
              '->startup->get returns nothing' );
}

{
   my $f = $cass->options;

   my $stream = "";
   wait_for_stream { length $stream >= 8 } $S2 => $stream;

   # OPCODE_OPTIONS
   is_hexstr( $stream,
              "\x01\x00\x01\x05\0\0\0\0",
              'stream after ->options' );

   # OPCODE_READY
   $S2->syswrite( "\x81\x00\x01\x06\0\0\0\x2f\0\2" .
                  "\x00\x0bCOMPRESSION\0\1\x00\x06snappy" .
                  "\x00\x0bCQL_VERSION\0\1\x00\x053.0.0" );

   wait_for { $f->is_ready };

   is_deeply( scalar $f->get,
              { COMPRESSION => ["snappy"], CQL_VERSION => ["3.0.0"] },
              '->options->get returns HASH of options' );
}

done_testing;
