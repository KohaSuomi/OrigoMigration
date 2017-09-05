#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use DBD::mysql;
use Data::Dumper;
use File::Basename;
use POSIX qw{strftime};
use Encode;

# CONFIG VARIABLES
my $user = "user";
my $pw = "password";

# DATA SOURCE NAME

my $dsn_temp = "dbi:mysql::10.0.3.12:3306";

# PERL DBI CONNECT
my $dbh = DBI->connect($dsn_temp, $user, $pw);

my $sourcedb = "temp";


my $start = time();

#$dbh->("CREATE DATABASE IF NOT EXISTS ".$sourcedb);

my $destination = "/mnt/jako/import-scripts.sql";
unlink $destination;

my @files = </mnt/jako/final/hankinta.txt>;
foreach my $file (@files) {
    print $file . "\n";
    my $filename = basename($file,  ".txt");
    
     
    open (my $fh, '<',$file)  or die "cant open the file: $! \n";
    my @array = <$fh>;

    my $firstrow = shift (@array);

    my @fields;
    print "$firstrow\n";
    if ($firstrow =~ s//\t/g) {
        $firstrow =~ s/\t/,/g;
        $firstrow =~ s/,,,/;/g;
        $firstrow =~ s/,//g;
        @fields = split /;/, $firstrow;
    }elsif ($firstrow =~ s//;/g) {
        @fields = split /;/, $firstrow;
    } else {
        @fields = split /\t/, $firstrow;
    }

    my $drop = "DROP TABLE IF EXISTS ".$sourcedb.".".$filename.";";

    $dbh->do($drop);

    my $table = "CREATE TABLE ".$sourcedb.".".$filename." \n(";
    $table .= "Id int(11) NOT NULL AUTO_INCREMENT,\n";

    my $import = "LOAD DATA LOW_PRIORITY LOCAL INFILE '/mnt/jako/final/".$filename.".txt'
    REPLACE INTO TABLE ".$sourcedb.".".$filename." 
    CHARACTER SET latin1 
    FIELDS TERMINATED BY '+t'
    LINES TERMINATED BY '+n'
    IGNORE 1 LINES 
    (";

    my $insertfields = "(";
    my @keys;
    push @keys, "tauluID";
    foreach my $field (@fields)
    {
        $field =~ s/\x00//g;
        if(  \$field == \$fields[0]  ) {
            $field = 'tauluID';
            $table .=  $field." varchar(150) DEFAULT NULL,\n";
            $import .= $field.",";
            $insertfields .= $field.",";
        } elsif (\$field == \$fields[-1]) {
            if ($field =~ /ID/i) {
                push @keys, $field;
            }
            $table .=  $field." varchar(150) DEFAULT NULL,\n";
            $insertfields .= $field;
            $import .= $field;
        } else {
            if ($field =~ /ID/i) {
                push @keys, $field;
            }
            $table .=  $field." varchar(150) DEFAULT NULL,\n";
            $insertfields .= $field.",";
            $import .= $field.",";
        }
        
        
    }

    foreach my $key (@keys) {
        $table .= " KEY ".$key." (".$key."),";
    }
     
      
    $table .= "PRIMARY KEY (Id)";
    $table .= ")
    ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci";

    $import .= ");\n";
    $insertfields .= ")";

    $dbh->do($table);

    $import =~ s/\+/\\/g;

    open (my $f, '>>',$destination)  or die "cant open the file: $! \n";
    print $f $import;
    close ($f);

    # foreach my $content (@array) {

    #     my $insert = "INSERT INTO ".$sourcedb.".".$filename." ".$insertfields;
    #     $insert .= " VALUES ('";
        
    #     #$content =~ s/;/','/g; 
    #     #$content =~ s/{//g;
    #     #$content =~ s/}//g;
    #     $content =~ s/\'/\`/g;
    #     $content =~ s/\x00//g;
    #     $content =~ s/\t/','/g;
    #     $content =~ s/\r\n//g;
    #     $insert .= $content."');";
    #     #print "$insert\n";

    #     $dbh->do($insert);

    # }

    close ($fh);
}

my $output = system("mysql --local-infile --host=10.0.3.12 --user=".$user." --password=".$pw." < ".$destination);
print "$output\n";
my $end = time();
my $time = $end - $start;
print "Time: ".strftime("\%H:\%M:\%S", gmtime($time))."\n";

sub encode_to {
    my ($value) = @_;
    return encode( 'UTF-8', $value );
}
