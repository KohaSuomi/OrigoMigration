#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use DBD::mysql;
use Data::Dumper;
use File::Basename;
use POSIX qw{strftime};
use Encode;

###### PROBABLY NOT NECESSARY IF THESE ARE DEFINED ON OTHER MIGRATION #####

# CONFIG VARIABLES
my $user = "user";
my $pw = "password";

# DATA SOURCE NAME

my $dsn_temp = "dbi:mysql::10.0.3.12:3306";

# PERL DBI CONNECT
my $dbh = DBI->connect($dsn_temp, $user, $pw);

my $destinationdb = "koha";
my $sourcedb = "temp";


my $start = time();

my @files = </mnt/jako/niteentilat.txt>;
foreach my $file (@files) {
    print $file . "\n";
    my $filename = basename($file,  ".txt");
    my $category;
    my $tablename;

    if ($filename eq "hyllyt") {
        $category = "CCODE";
        $tablename = "hyllyt";
    }
    if ($filename eq "osastot") {
        $category = "LOC";
        $tablename = "osastot";
    }
    if ($filename eq "materiaalit") {
        $tablename = "materiaalit";
    }
    if ($filename eq "niteentilat") {
        $tablename = "niteentilat";
    }
     
    open (my $fh, '<',$file)  or die "cant open the file: $! \n";
    my @array = <$fh>;

    foreach my $field (@array)
    {
        my $lstart = "<lyhytnimi>";
        my $lend = "</lyhytnimi>";
        my $pstart = "<pitkanimi>";
        my $pend = "</pitkanimi>";
        $field = parse_data($field);
        
        #print Dumper $field =~ /$lstart(.*?)$lend/;
        #print Dumper $field =~ /$pstart(.*?)$pend/;
        if ($field =~ /$lstart(.*?)$lend/ && $field =~ /False/i) {
            #print Dumper $field =~ /(\d+)/;
            unless ($filename eq "materiaalit" || $filename eq "niteentilat") {
                insert_database("authorised_values", $category, $field =~ /$lstart(.*?)$lend/, $field =~ /$pstart(.*?)$pend/);
            }
            
            insert_temp($tablename,$field =~ /(\d+)/, $field =~ /$lstart(.*?)$lend/, $field =~ /$pstart(.*?)$pend/ );
        } elsif ($field =~ /$lstart(.*?)$lend/) {
            unless ($filename eq "materiaalit" || $filename eq "niteentilat") {
                insert_database("authorised_values", $category, $field =~ /$lstart(.*?)$lend/, $field =~ /$pstart(.*?)$pend/);
            }
            insert_temp($tablename,$field =~ /(\d+)/, $field =~ /$lstart(.*?)$lend/, $field =~ /$pstart(.*?)$pend/ );
        }
        
        
        
        
    }
}
my $end = time();
my $time = $end - $start;
print "Time: ".strftime("\%H:\%M:\%S", gmtime($time))."\n";


sub insert_database {
    my ($tablename, $category, $value, $lib) = @_;
    my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
    $insert .= "category, authorised_value, lib, lib_opac) VALUES (";
    $insert .= "'".$category."', '".$value."', '".$lib."', '".$lib."');";
    $dbh->do($insert);
}

sub insert_temp {
    my ($tablename, $id, $value, $lib) = @_;
    my $insert = "INSERT INTO ".$sourcedb.".".$tablename." (";
    $insert .= "Id, lyhenne, txt) VALUES (";
    $insert .= "'".$id."', '".$value."', '".$lib."');";
    $dbh->do($insert);
}

sub parse_data {
    my ($data) = @_;
    $data =~ s/\x00//g;
    return $data;
}

sub encode_to {
    my ($value) = @_;
    return encode( 'UTF-8', $value );
}
