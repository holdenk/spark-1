my @hadoop_version = ("2.5.0-cdh5.3.2", "2.4.0", "2.4.1-mapr-1408", "h2.2.0-gphd-3.1.0.0", "2.3.0-cdh5.1.2", "2.6.0", "2.5.1");
for my $version @hadoop_version {
    my $shortVersion = $version;
    $shortVersion =~ s/\-.*+//;
    print "Building for $version with hadoop=$shortVersion\n";
    `perl update-version.pl $version`;
    `./build/sbt ./build/sbt -Pyarn -Phive -Phadoop=$shortVersion -Phive-thriftserver -Dhadoop.version=$version clean compile package assembly publishLocal`;
}
