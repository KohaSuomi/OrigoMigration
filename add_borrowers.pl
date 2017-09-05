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

my $destination = "OrigoMigration/import-ssn.sql";
unlink $destination;

open (my $fh, '>>',$destination)  or die "cant open the file: $! \n";
print $fh "ALTER TABLE ssn AUTO_INCREMENT=10000000;\n";
close ($fh);


$dbh->do("set foreign_key_checks = 0;");
# $dbh->do("truncate koha_siilinjarvi.borrowers;");
# $dbh->do("truncate koha_siilinjarvi.borrower_message_preferences;");
# $dbh->do("truncate koha_siilinjarvi.borrower_message_transport_preferences;");
# $dbh->do("truncate koha_siilinjarvi.borrower_attributes;");
# $dbh->do("truncate koha_siilinjarvi.messages;");
# $dbh->do("truncate koha_siilinjarvi.borrower_debarments;");
my $start = time();

my $asiakkaat=$dbh->prepare("SELECT tauluID, KontaktiID, TakaajaID, Salasana, Asiakastyyppi, Saapumisilmoitustapa, Muistutustapa, Muistutusraja, Kayttokieli FROM ".$sourcedb.".asiakkaat where Del = 'False';");
$asiakkaat->execute;

my $row = 0;

while (my $borrower = $asiakkaat->fetchrow_hashref){
    $row++;
    my $borrower_data;
    $borrower_data = get_borrower($borrower->{KontaktiID});
    $borrower_data->{TakaajaID} = $borrower->{TakaajaID};
    $borrower_data->{Salasana} = $borrower->{Salasana};
    my $diff = $today->output('iso') - parse_date($borrower_data->{Syntymaaika});
    if ($diff <= 18) {
        $borrower_data->{Asiakastyyppi} = 'LAPSI';
        $borrower_data->{Guarantor} = get_borrower($borrower->{TakaajaID});
    } else {
        $borrower_data->{Asiakastyyppi} = category_change($borrower->{Asiakastyyppi});
    }
    $borrower_data->{Sukupuoli} = sex_change($borrower_data->{Sukupuoli});
    $borrower_data->{Address} = get_address($borrower->{KontaktiID}, 0);
    $borrower_data->{Address2} = get_address($borrower->{KontaktiID}, 1);
    $borrower_data->{Phone} = get_phone($borrower->{KontaktiID}, 2);
    $borrower_data->{Mobile} = get_phone($borrower->{KontaktiID}, 1);
    $borrower_data->{B_phone} = get_phone($borrower->{KontaktiID}, 0);
    if (get_barcode($borrower->{tauluID}, $borrower_data->{Tunnus}, "0")) {
        $borrower_data->{barcode} = get_barcode($borrower->{tauluID}, $borrower_data->{Tunnus}, "0");
    } elsif (get_barcode($borrower->{tauluID}, $borrower_data->{Tunnus}, "1")) {
        $borrower_data->{barcode} = get_barcode($borrower->{tauluID}, $borrower_data->{Tunnus}, "1");
    } elsif (get_barcode($borrower->{tauluID}, $borrower_data->{Tunnus}, "2")) {
        $borrower_data->{barcode} = get_barcode($borrower->{tauluID}, $borrower_data->{Tunnus}, "2");
    }

    my $borrowernumber = insert_borrowers("borrowers", $borrower_data);

    if (_validateSsn(parse_data($borrower_data->{Tunnus}))) {
        insert_ssn($borrowernumber, parse_data($borrower_data->{Tunnus}));
    }
    if(parse_data($borrower->{Kayttokieli})) {
        insert_language($borrowernumber, parse_data($borrower->{Kayttokieli}));
    }
    set_notes($borrower->{tauluID}, $borrowernumber);

    set_messages($borrowernumber, parse_data($borrower->{Saapumisilmoitustapa}), "Saapumisilmoitustapa", undef);
    set_messages($borrowernumber, parse_data($borrower->{Muistutustapa}), "Muistutustapa", parse_data($borrower->{Muistutusraja}));

}
my $guarantor = set_guarantor();
my $encrypted = encrypt_password();
if ($encrypted) {
   print "Encrypted passwords\n";
}

my $end = time();
my $time = $end - $start;
print "Time: ".strftime("\%H:\%M:\%S", gmtime($time))."\n";


sub insert_borrowers {
    my ($tablename, $source) = @_;

    unless (parse_data($source->{Phone}) =~ /^+358/) {
        $source->{Phone} =~ s/\D//g;
    }
    
    my @fields;
    push @fields, 'borrowernumber';
    if ($source->{barcode}) {
        push @fields, 'cardnumber';
        push @fields, 'userid';
        push @fields, 'othernames';
    }

    push @fields, (
    'surname',
    'firstname',
    'address',
    'city',
    'zipcode',
    'email',
    'dateofbirth',
    'branchcode',
    'categorycode',
    'dateenrolled',
    'dateexpiry',
    'contacttitle',
    'contactfirstname',
    'contactname',
    'sex',
    'password',
    'phone',
    'mobile',
    'smsalertnumber',
    'B_phone',
    'borrowernotes');

    my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
    foreach my $field (@fields) {
        if (\$field == \$fields[-1]) {
            $insert .= $field."";
        }else {
            $insert .= $field.",";
        }
        
    }
    #print "Barcode: ".$source->{barcode}."\n";
    $insert .= ") VALUES (";
    $insert .= "'".$source->{borrowernumber}."',";
    if ($source->{barcode}) {
        $insert .= "'".parse_data($source->{barcode})."',";
        $insert .= "'".parse_data($source->{barcode})."',";
        $insert .= "'".parse_data($source->{barcode})."',";
    }
    $insert .= "'".parse_data($source->{Sukunimi})."',";
    $insert .= "'".parse_data($source->{Etunimet})."',";
    $insert .= "'".parse_data($source->{Address}->{Katuosoite})."',";
    $insert .= "'".parse_data($source->{Address}->{Postitoimipaikka})."',";
    $insert .= "'".parse_data($source->{Address}->{Postinumero})."',";
    $insert .= "'".parse_data($source->{Sahkoposti})."',";
    $insert .= "'".parse_date($source->{Syntymaaika})."',";
    $insert .= "'SII_PK',";
    $insert .= "'".parse_data($source->{Asiakastyyppi})."',";
    $insert .= "current_date,",
    $insert .= "date_add(current_date, interval 3 year),";
    $insert .= "'".parse_data($source->{TakaajaID})."',";
    $insert .= "'".parse_data($source->{Guarantor}->{Etunimet})."',";
    $insert .= "'".parse_data($source->{Guarantor}->{Sukunimi})."',";
    $insert .= "'".parse_data($source->{Sukupuoli})."',";
    $insert .= "'".parse_data($source->{Salasana})."',";
    $insert .= "'".parse_data($source->{Phone})."',";
    $insert .= "'".parse_data($source->{Mobile})."',";
    $insert .= "'".parse_data($source->{Phone})."',";
    $insert .= "'".parse_data($source->{B_phone})."',";
    $insert .= "'".parse_data($source->{Lisatiedot})."'";
    $insert .= ")";

    $dbh->do($insert);

    return $dbh->{mysql_insertid};
   
}
sub parse_data {
    my ($data) = @_;

    #$data =~ s/\x00//g;
    $data =~ s/[[:cntrl:]]+//g;

    return $data;
}
sub parse_date {
    my ($date) = @_;

    $date =~ s/\x00//g;
    my ($y, $m, $d) = $date =~ /^(\d\d\d\d)-(\d\d)-(\d\d)/;
    my $bdate = $y."-".$m."-".$d;

    return $bdate;
}

sub get_borrower {
    my ($id) = @_;
    $id =~ s/\x00//g;
    my $query = "SELECT * FROM ".$sourcedb.".kontakti where tauluID = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);

    return $sth->fetchrow_hashref;

}

sub get_address {
    my ($id, $order) = @_;
    $id =~ s/\x00//g;
    $order =~ s/\x00//g;
    my $query = "SELECT Katuosoite, Postitoimipaikka, Postinumero FROM ".$sourcedb.".osoitteet where KontaktiID = ? and REPLACE(Jarjestys,'\r','') = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id, $order);

    return $sth->fetchrow_hashref;

}

sub get_phone {
    my ($id, $order) = @_;
    $id =~ s/\x00//g;
    $order =~ s/\x00//g;
    my $query = "SELECT Numero FROM ".$sourcedb.".puhelin where KontaktiID = ? and REPLACE(Tyyppi,'\r','') = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id, $order);

    return $sth->fetchrow;

}

sub set_notes {
    my ($id, $borrowernumber) = @_;
    $id =~ s/\x00//g;
    my $query = "SELECT Syy, Paivamaara, Lainauskielto FROM ".$sourcedb.".lainakielto where AsiakasID = ? and Del = 'False'";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);

    my $query2 = "INSERT INTO ".$destinationdb.".messages (borrowernumber, branchcode, message_type, message, message_date) VALUES (?,?,?,?,?)";
    while (my ($note, $date, $debarment) = $sth->fetchrow) {
       if (parse_data($debarment) eq "True") {
            my $debar=$dbh->prepare("INSERT INTO ".$destinationdb.".borrower_debarments (borrowernumber, type, comment, created) VALUES (?,?,?,?)");
            $debar->execute($borrowernumber, "MANUAL", parse_data($note), parse_date($date));
            my $debar1=$dbh->prepare("UPDATE ".$destinationdb.".borrowers set debarred = ?, debarredcomment = ? where borrowernumber = ?;");
            $debar1->execute("9999-12-31", parse_data($note), $borrowernumber);
       } else {
            my $insert = $dbh->prepare($query2);
            $insert->execute($borrowernumber, 'SII_PK', 'L', parse_data($note), parse_date($date));
       }
    }

}

sub get_barcode {
    my ($id, $ssn, $order) = @_;
    $id =~ s/\x00//g;
    $ssn =~ s/\x00//g;
    my $query = "SELECT tauluID FROM ".$sourcedb.".asiakasviivakoodit where AsiakasID = ? and tauluID != ? and Jarjestys = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id, $ssn, $order);

    return $sth->fetchrow;

}

sub set_guarantor {
    my $query = "SELECT borrowernumber, contacttitle FROM ".$destinationdb.".borrowers where contacttitle != '' and contactname != ''";
    my $sth=$dbh->prepare($query);
    $sth->execute();
    while (my ($borrowernumber, $guarantor) = $sth->fetchrow) {
        my $borrower = get_borrower($guarantor);
        my $query1 = "SELECT borrowernumber FROM ".$destinationdb.".borrowers where surname = ? and firstname = ? and dateofbirth = ?";
        my $sth1=$dbh->prepare($query1);
        $sth1->execute(parse_data($borrower->{Sukunimi}), parse_data($borrower->{Etunimet}), parse_date($borrower->{Syntymaaika}));
        my $guarantorid = $sth1->fetchrow;
        update_guarantor($guarantorid, $borrowernumber);
    }
    

    return 1;

}

sub update_guarantor {
    my ($guarantorid, $borrowernumber) = @_;
    my $query = "UPDATE ".$destinationdb.".borrowers set guarantorid = ? where borrowernumber = ?;";
    my $sth=$dbh->prepare($query);
    $sth->execute($guarantorid, $borrowernumber);
}

sub set_messages {
    my ($borrowernumber, $type, $message, $limit) = @_;
    $type =~ s/\x00//g;
    if($type ne "0" && $type ne "" && $type ne "1" && $message eq "Saapumisilmoitustapa") {
        insert_message_preferences($borrowernumber, $type, '4', undef);
    }
    if ($type eq "1" && $message eq "Muistutustapa") {
        insert_message_preferences($borrowernumber, 'email', '2', $limit);
    }
    if ($type eq "2" && $message eq "Muistutustapa") {
        insert_message_preferences($borrowernumber, 'sms', '2', $limit);
    }

}

sub insert_message_preferences {
    my ($borrowernumber, $type, $attribute, $limit) = @_;
    my $query = "INSERT INTO ".$destinationdb.".borrower_message_preferences 
    (borrower_message_preferences.borrowernumber,
    borrower_message_preferences.message_attribute_id";
    if ($limit) {
        $query .= ", days_in_advance, wants_digest) ";
        $query .= "VALUES (?, ?, ?, 1)";
    } else {
        $query .= ") VALUES (?, ?)";
    }
    my $sth=$dbh->prepare($query);
    if ($limit) {
        $sth->execute($borrowernumber, $attribute, $limit);
    }else {
        $sth->execute($borrowernumber, $attribute);
    }
    my $rv = $dbh->{mysql_insertid};
    insert_message_transport_preferences($rv, $type);
}

sub insert_message_transport_preferences {
    my ($id, $type) = @_;
    my $query = "INSERT INTO ".$destinationdb.".borrower_message_transport_preferences 
    (borrower_message_transport_preferences.borrower_message_preference_id,
    borrower_message_transport_preferences.message_transport_type) VALUES (?,?)";
    my $sth=$dbh->prepare($query);
    $sth->execute($id, message_types($type));

}

sub encrypt_password {

    my $borrowers=$dbh->prepare("SELECT borrowernumber, userid, password FROM ".$destinationdb.".borrowers b WHERE b.password != ''");
    $borrowers->execute;

    while (my ($borrowernumber,$userid,$password)= $borrowers->fetchrow){
        ModMember(borrowernumber => $borrowernumber, password => $password);
        #print "Borrower : $borrowernumber, $password\n";
    }
    return 1;
}

sub insert_language {
    my ($borrowernumber, $language) = @_;
    
    my $query = "INSERT INTO ".$destinationdb.".borrower_attributes (borrowernumber, code, attribute) VALUES (?,?,?)";
    
    my $insert = $dbh->prepare($query);
    $insert->execute($borrowernumber, 'USER_LANG', $language);

}

sub insert_ssn {
    my ($borrowernumber, $borrower_ssn) = @_;

    my $ssnkey = "1";
    $ssnkey .= sprintf("%07d", $borrowernumber);
    
    my $query = "INSERT INTO ".$destinationdb.".borrower_attributes (borrowernumber, code, attribute) VALUES (?,?,?)";
    
    my $insert = $dbh->prepare($query);
    $insert->execute($borrowernumber, 'SSN', 'sotu'.$ssnkey);

    my $import = "INSERT IGNORE INTO ssn (ssnkey, ssnvalue) VALUES (".$ssnkey.",'".$borrower_ssn."');\n";
    
    open (my $f, '>>',$destination)  or die "cant open the file: $! \n";
    print $f $import;
    close ($f);
}

sub _validateSsn {
    my $ssnvalue = shift;

    #Valid check marks.
    my $checkmarkvalues = "0123456789ABCDEFHJKLMNPRSTUVWXY";

    if ($ssnvalue =~ /(\d{6})[-+AB](\d{3})(.)/) {
        my $digest = $1.$2;
        my $checkmark = $3;

        my $checkmark_index = $digest % 31;
        my $checkmark_expected = substr $checkmarkvalues, $checkmark_index, 1;

        if ($checkmark eq $checkmark_expected) {
            return 1;
        }
    }
    return 0;
}

sub message_types {
    my ($type) = @_;
    my $new_type = $type;
    if ($type eq "1") {
        $new_type = "none";
    }
    if ($type eq "2") {
        $new_type = "email";
    }
    if ($type eq "3") {
        $new_type = "sms";
    }
    if ($type eq "4") {
        $new_type = "email";
    }
    return $new_type;
}

sub category_change {
    my ($borrowercategory) = @_;

    $borrowercategory =~ s/\x00//g;
    if ($borrowercategory eq '1') {
        return "TAKAAJA";
    }
    if ($borrowercategory eq '2') {
        return "HENKILO";
    }
    if ($borrowercategory eq '3') {
        return "YHTEISO";
    }
    if ($borrowercategory eq '4') {
        return "KAUKO2";
    }
    if ($borrowercategory eq '5') {
        return "KOTIPAL";
    }
    if ($borrowercategory eq '6') {
        return "KAUKO1";
    }
    if ($borrowercategory eq '8') {
        return "VIRKAILIJA";
    }

    return $borrowercategory;
}

sub library_change {
    my ($branch) = @_;

    $branch =~ s/\x00//g;
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

sub sex_change {
    my ($sex) = @_;

    $sex =~ s/\x00//g;
    if ($sex eq '1') {
        return "M";
    }
    if ($sex eq '2') {
        return "F";
    }

    return $sex;
}


