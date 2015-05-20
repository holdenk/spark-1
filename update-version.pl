#!/usr/bin/perl
use strict;
use warnings;
use File::Slurp;
open (IN, "find ./ -name pom.xml|");
while (my $filename = <IN>) {
    chomp($filename);
    my $file = read_file($filename);
    $file =~ s/version>([\d\.\-]+)(SNAPSHOT|)<\/version/version>$1$2-ALPINE<\/version/;
    print $file;
    open (OUT, ">$filename");
    print OUT $file;
    close (OUT);
}
