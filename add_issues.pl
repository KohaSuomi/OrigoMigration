#!/usr/bin/perl

use strict;
#use warnings;
use DBI;
use DBD::mysql;
use Data::Dumper;
use File::Basename;
use POSIX qw{strftime};
use C4::Members;

# CONFIG VARIABLES
my $user = "user";
my $pw = "password";

# DATA SOURCE NAME

my $dsn_temp = "dbi:mysql::10.0.3.12:3306";

my $destinationdb = "koha";
my $sourcedb = "temp";

# PERL DBI CONNECT
my $dbh = DBI->connect($dsn_temp, $user, $pw);

my $today = C4::Dates->new();


$dbh->do("set foreign_key_checks = 0;");
# $dbh->do("truncate ".$destinationdb.".issues;");
my $start = time();

my $issues=$dbh->prepare("SELECT 
AsiakasID,
NideID,
Lainauspiste,
Lainausaika,
Erapaiva,
Uusimiskerta FROM ".$sourcedb.".lainat_uusi where Palautettu = '0';");
$issues->execute;

my $row = 0;

while (my $issue = $issues->fetchrow_hashref){
    $issue->{itemnumber} = get_item($issue->{NideID});
    $issue->{borrowernumber} = get_borrower($issue->{AsiakasID});
    $issue->{Lainauspiste} = library_change($issue->{Lainauspiste});
    #print Dumper $issue;
    if ($issue->{itemnumber} && $issue->{borrowernumber}) {
        insert_issues("issues", $issue);
    }

}

my $end = time();
my $time = $end - $start;
print "Time: ".strftime("\%H:\%M:\%S", gmtime($time))."\n";


sub insert_issues {
    my ($tablename, $source) = @_;

    my @fields;

    push @fields, (
    'borrowernumber',
    'itemnumber',
    'date_due',
    'renewals',
    'issuedate',
    'branchcode');

    my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
    foreach my $field (@fields) {
        if (\$field == \$fields[-1]) {
            $insert .= $field."";
        }else {
            $insert .= $field.",";
        }
        
    }
    $insert .= ") VALUES (";
    $insert .= "'".$source->{borrowernumber}."',";
    $insert .= "'".parse_data($source->{itemnumber})."',";
    $insert .= "'".parse_data($source->{Erapaiva})."',";
    $insert .= "'".parse_data($source->{Uusimiskerta})."',";
    $insert .= "'".parse_data($source->{Lainausaika})."',";
    $insert .= "'".parse_data($source->{Lainauspiste})."'";
    $insert .= ")";
    
    #print "$insert\n";
    $dbh->do($insert);

    #return $dbh->{mysql_insertid};
   
}

sub get_item {
    my ($id) = @_;
    #$id =~ s/\x00//g;
    $id = "{".$id."}";
    my $query = "SELECT Viivakoodi FROM ".$sourcedb.".niteet where tauluID = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);
    my $barcode = parse_data($sth->fetchrow);

    my $query2 = "SELECT itemnumber FROM ".$destinationdb.".items where barcode = ?";
    my $sth2=$dbh->prepare($query2);
    $sth2->execute($barcode);
    return $sth2->fetchrow;

}

sub get_borrower {
    my ($id) = @_;
    #$id =~ s/\x00//g;
    $id = "{".$id."}";
    my $query = "SELECT tauluID FROM ".$sourcedb.".asiakasviivakoodit where AsiakasID = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);
    my $cardnumber = parse_data($sth->fetchrow);

    my $query2 = "SELECT borrowernumber FROM ".$destinationdb.".borrowers where cardnumber = ?";
    my $sth2=$dbh->prepare($query2);
    $sth2->execute($cardnumber);
    my $data = $sth2->fetchrow;
    unless ($data) {
        my $query1 = "SELECT Sukunimi, Etunimet, Syntymaaika FROM ".$sourcedb.".asiakkaat a 
        JOIN ".$sourcedb.".kontakti k ON a.KontaktiID = k.tauluID 
        where a.tauluID = ?";
        my $sth1=$dbh->prepare($query1);
        $sth1->execute($id);
        my $asiakas = $sth1->fetchrow_hashref;
        my $query3 = "SELECT borrowernumber FROM ".$destinationdb.".borrowers where surname = ? and firstname = ? and dateofbirth = ?";
        my $sth3=$dbh->prepare($query3);
        $sth3->execute(parse_data($asiakas->{Sukunimi}), parse_data($asiakas->{Etunimet}), parse_date($asiakas->{Syntymaaika}));
        my $borr = $sth3->fetchrow;
        print "$borr\n";
        return $borr;
    } else {
        return $data;
    }

    # my $query2 = "SELECT borrowernumber FROM ".$destinationdb.".borrowers where cardnumber = ?";
    # my $sth2=$dbh->prepare($query2);
    # $sth2->execute($cardnumber);
    # return $sth2->fetchrow;

}

sub library_change {
    my ($branch) = @_;

    $branch = parse_data($branch);
    if ($branch eq '1') {
        return "SII_PK";
    }
    if ($branch eq '3') {
        return "SII_AU";
    }
    if ($branch eq '4') {
        return "SII_VU";
    }

    return $branch;
}

sub parse_data {
    my ($data) = @_;
    $data =~ s/\x00//g;
    return trim($data);
}

sub parse_date {
    my ($date) = @_;

    $date =~ s/\x00//g;
    my ($y, $m, $d) = $date =~ /^(\d\d\d\d)-(\d\d)-(\d\d)/;
    my $bdate = $y."-".$m."-".$d;

    return $bdate;
}

sub  trim { 
    my $s = shift; 
    $s =~ s/^\s+|\s+$//g; 
    return $s 
};