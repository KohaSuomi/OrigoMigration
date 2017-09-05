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

my $destinationdb = "koha_siilinjarvi";
my $sourcedb = "siilinjarvi_temp";

# PERL DBI CONNECT
my $dbh = DBI->connect($dsn_temp, $user, $pw);

my $today = C4::Dates->new();


$dbh->do("set foreign_key_checks = 0;");
$dbh->do("truncate ".$destinationdb.".accountlines;");
my $start = time();

my $accountlines=$dbh->prepare("SELECT 
tauluID,
AsiakasID,
NideID,
Luontipaiva,
Maksumaara,
Lisatiedot,
Tyyppi FROM ".$sourcedb.".maksut where Del = 'False';");
$accountlines->execute;

my $row = 0;

while (my $accountline = $accountlines->fetchrow_hashref){
    $accountline->{itemnumber} = get_item($accountline->{NideID});
    $accountline->{borrowernumber} = get_borrower($accountline->{AsiakasID});
    #$accountline->{Tyyppi} = type_change($accountline->{Tyyppi}, $accountline->{tauluID});
    
    if ($accountline->{borrowernumber} && parse_data($accountline->{Maksumaara}) =~ /^((?!0,00).)*$/) {
        #print Dumper $accountline;
        insert_accountlines("accountlines", $accountline);
    }
    

}

my $end = time();
my $time = $end - $start;
print "Time: ".strftime("\%H:\%M:\%S", gmtime($time))."\n";


sub insert_accountlines {
    my ($tablename, $source) = @_;

    my $payment = parse_data($source->{Maksumaara});
    $payment =~ tr/,/./;

    my $desc = parse_data($source->{Lisatiedot});
    $desc =~ s/\'/\`/g;

    my @fields;

    push @fields, (
    'borrowernumber',
    'itemnumber',
    'date',
    'amount',
    'description',
    'note',
    'amountoutstanding');

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
    $insert .= "'".parse_date($source->{Luontipaiva})."',";
    $insert .= "'".$payment."',";
    $insert .= "'".$desc."',";
    $insert .= "'".$desc."',";
    $insert .= "'".$payment."'";
    $insert .= ")";
    
    #print "$insert\n";
    $dbh->do($insert);

    #return $dbh->{mysql_insertid};
   
}

sub get_item {
    my ($id) = @_;
    $id =~ s/\x00//g;
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
    $id =~ s/\x00//g;
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
        #print "$borr\n";
        return $borr;
    } else {
        return $data;
    }

}

sub type_change {
    my ($type, $id) = @_;
    $id =~ s/\x00//g;
    $type = parse_data($type);
    if ($type eq '1') {
        return "Varausmaksu";
    }
    if ($type eq '8') {
        return "Myöhästymismaksu";
    }
    # if ($type eq '3') {
    #     $id =~ s/\x00//g;
    #     my $query = "SELECT huomautusluokka FROM ".$sourcedb.".maksut where tauluID = ?";
    #     my $sth=$dbh->prepare($query);
    #     $sth->execute($id);
    #     my $value = $sth->fetchrow;
    #     if ($value eq "1") {
    #         return "1. huomautus";
    #     }
    #     if ($value eq "2") {
    #         return "2. huomautus";
    #     }
    #     if ($value eq "2") {
    #         return "2. huomautus";
    #     }
        
    # }

    return $type;
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