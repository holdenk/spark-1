#!/usr/bin/perl
use strict;
use warnings;
use File::Slurp;
open (IN, "find ./ -name pom.xml|");
while (my $filename = <IN>) {
    chomp($filename);
    my $file = read_file($filename);
    $file =~ s/version>(1.3.1-SNAPSHOT)(-ALPINE|)<\/version/version>$1$2-HOLDEN<\/version/;
    open (OUT, ">$filename");
    print OUT $file;
    close (OUT);
}
