#!/usr/bin/perl

use strict;
#use warnings;
use DBI;
use DBD::mysql;
use Data::Dumper;
use POSIX qw{strftime};
use C4::Dates;
use Encode;

# CONFIG VARIABLES
my $user = "user";
my $pw = "password";

# DATA SOURCE NAME

my $dsn_temp = "dbi:mysql::10.0.3.12:3306";

my $destinationdb = "koha";
my $sourcedb = "temp";

my $destination = "OrigoMigration/MARC/finmarc.xml";

# PERL DBI CONNECT
my $dbh = DBI->connect($dsn_temp, $user, $pw);


my $today = C4::Dates->new();

my $start = time();

my $teokset=$dbh->prepare("SELECT tauluId, Nimeke, Paasana, Tunnus, Julkaisuaika, Nimio FROM ".$sourcedb.".teokset where Del = '0';");
$teokset->execute;

my $teokset_rest = $dbh->prepare("select t.tauluId, t.Nimio from ".$destinationdb.".items i
join ".$sourcedb.".niteet n on i.barcode = n.Viivakoodi
join ".$sourcedb.".teokset_uusi t on n.TeosID = concat('{',t.tauluId,'}')
where i.biblionumber = 0;");

$teokset_rest->execute;

my $row = 0;

open my $fh, '>', $destination or die "Cannot open finmarc.xml: $!";
print $fh "<?xml version=\"1.0\" ?>\n";

while (my $biblio = $teokset_rest->fetchrow_hashref){
    print Dumper $biblio;
    my $marc = get_marc($biblio->{tauluId}, parse_data($biblio->{Nimio}));
    #print Dumper $marc;
    print $fh $marc;
    $row++;

   
}

close $fh;

#Käytetään myöhemmin niteiden ja tietueiden yhdistelyyn.
#dbh->do("ALTER TABLE ".$destinationdb.".biblioitems ADD INDEX collectiontitle (collectiontitle(50) ASC)");

my $end = time();
my $time = $end - $start;
print "Total: $row\n";
print "Time: ".strftime("\%H:\%M:\%S", gmtime($time))."\n";

sub get_marc {
    my ($id, $nimio) = @_;
    $id =~ s/\x00//g;
    $id =~ s/{//g;
    $id =~ s/}//g;
    my $query = "SELECT Kentta, KentanIndeksi, Indikaattori1, Indikaattori2, Osakentta, OsakentanIndeksi, Sisalto FROM ".$sourcedb.".marc_uusi where TeosID = ? and Kentta != '35'";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);
    my @marcs;
    my @field;
    my @index;
    my $subfields;
    my $fields;
    $fields = "<record>\n";
    $fields .= "\t<leader>".$nimio."</leader>\n";
    while (my $marc = $sth->fetchrow_hashref) {
    	$marc->{TeosID} = $id;
        my $subfield = parse_data($marc->{Osakentta});
        my $last_field = pop @field;
        my $last_index = pop @index;
        my $ind1;
        my $ind2;
        if (parse_data($marc->{Indikaattori1}) ne "255") {
            $ind1 = parse_data($marc->{Indikaattori1});
        }
        if (parse_data($marc->{Indikaattori2}) ne "255") {
            $ind2 = parse_data($marc->{Indikaattori2});
        }
        if (parse_data($marc->{Kentta}) eq '1') {
            $fields .= "\t\<controlfield tag=\"".leading_zeros($marc->{Kentta})."\">".parse_data($marc->{Sisalto})."</controlfield>\n";
        } elsif (parse_data($marc->{Kentta}) eq '8') {
            $fields .= "\t\<controlfield tag=\"".leading_zeros($marc->{Kentta})."\">".parse_data($marc->{Sisalto})."</controlfield>\n";
        } else {
            if (parse_data($marc->{Kentta}) ne $last_field) {
                $fields .= "\t</datafield>\n" if $last_field ne "8" && $last_field ne "1";
                $fields .= "\t<datafield tag=\"".leading_zeros($marc->{Kentta})."\" ind1=\"".$ind1."\" ind2=\"".$ind2."\">\n";
                $fields .= "\t\t<subfield code=\"".$subfield."\">".parse_data($marc->{Sisalto})."</subfield>\n";
            } else {
                if (parse_data($marc->{KentanIndeksi}) ne $last_index) {
                    $fields .= "\t</datafield>\n" if $last_field ne "8" || $last_field ne "1";
                    $fields .= "\t<datafield tag=\"".leading_zeros($marc->{Kentta})."\" ind1=\"".$ind1."\" ind2=\"".$ind2."\">\n";
                    $fields .= "\t\t<subfield code=\"".$subfield."\">".parse_data($marc->{Sisalto})."</subfield>\n";
                } else {
                    $fields .= "\t\t<subfield code=\"".$subfield."\">".parse_data($marc->{Sisalto})."</subfield>\n";
                }
                
            }

        }

    	push @field, parse_data($marc->{Kentta});
        push @index, parse_data($marc->{KentanIndeksi});
    }
    $fields .= "\t</datafield>\n";
    $fields .= "\t<datafield tag=\"35\" ind1=\"\" ind2=\"\">\n";
    $fields .= "\t\t<subfield code=\"a\">{".parse_data($id)."}</subfield>\n";
    $fields .= "\t</datafield>\n";
    $fields .= "</record>\n";
    return $fields;

}

sub create_xml_field {
    my ($field, $data, $last_field) = @_;
    my $xml_field;
    if ($field eq '8') {
        $xml_field->{fieldnumber} = "008";
        $xml_field->{content} = parse_data($data->{Sisalto});
    }
    elsif ($field eq '1') {
        $xml_field->{fieldnumber} = "001";
        $xml_field->{content} = parse_data($data->{TeosID});
    } else {
    	my $f = sprintf("%02d", parse_data($data->{Kentta}));
    	my $ind1;
    	my $ind2;
    	if (parse_data($data->{Indikaattori1}) ne "255") {
			$ind1 = parse_data($data->{Indikaattori1});
    	}
    	if (parse_data($data->{Indikaattori2}) ne "255") {
    		$ind2 = parse_data($data->{Indikaattori2});
    	}
        # if ($f eq $last_field && parse_data($data->{Osakentta})) {
        #     $xml_field->{subfiels} = [];
        # }
    	if ($field eq '35') {
            $xml_field->{fieldchar} = parse_data($data->{Osakentta});
            $xml_field->{content} = parse_data($data->{TeosID});
            $xml_field->{fieldnumber} = $f;
            $xml_field->{ind1} = $ind1;
            $xml_field->{ind2} = $ind2;
            $xml_field->{last_field} = $last_field;
    	} else {
            $xml_field->{fieldnumber} = $f;
            $xml_field->{ind1} = $ind1;
            $xml_field->{ind2} = $ind2;
            $xml_field->{fieldchar} = parse_data($data->{Osakentta});
            $xml_field->{content} = parse_data($data->{Sisalto});
            $xml_field->{last_field} = parse_data($last_field);
    	}
    }

    return $xml_field;

}

sub leading_zeros {
    my ($data) = @_;
    my $length = length parse_data($data);
    my $f;
    if ($length = 1) {
        $f = sprintf("%03d", parse_data($data));
        return $f;
    }
    if ($length = 2) {
        $f = sprintf("%02d", parse_data($data));
        return $f;
    }
    return parse_data($data);
}

sub parse_data {
    my ($data) = @_;

    #$data =~ s/\x00//g;
    $data =~ s/[[:cntrl:]]+//g;

    return encode_to($data);
}
sub encode_to {
    my ( $value ) = @_;
    my $new = decode('iso-8859-1', $value);
    return encode( 'UTF-8', $new );
}