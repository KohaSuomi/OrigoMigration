# Konversiotyökalu

## Ohjeita

- Käy vaihtamassa tietokantayhteydet tiedostoista ja lisää oikeat destinationdb ja sourcedb.
- Aja ensin add_origo_to_temp.pl ja tarkista vastaako temp-tietokanta tiedostojen sisältöä. Data siirretään väliaikaiseen tietokantaan, josta sitä aletaan siirtämään Kohaan. Tämä helpottaa korjaamista, koska voi palata yksittäiseen siirtoon uudestaan.
- Muokkaa tiedostojen sql-kyselyt vastaamaan omaa dumppiasi. Voi olla eroja esim. toimitetun ja itse ottaman dumpin välillä. Jos Koha-kannassa ei ole dataa entuudestaan voit poistaa vanhat tiedot truncate-komennolla, muuten pidä ne kommentoituina.
- Muuta kirjastoasi vastaavat arvot funktioihin skriptien sisällä.
- add_aqorders.pl tiedostossa on myös signumin luonti. Tarkista, että se luodaan oikein.

## Järjestys

1. add_origo_to_temp.pl
2. add_authorised_values.pl - ei pakollinen jos arvot tuotu Kohaan jo aikaisemmin.
3. add_borrowers.pl - Ennen ajoa tarkista: category_change, library_change.
4. create_marcxml.pl
5. Pura Usemarcon.tar.gz
6. ./usemarcon fi2ma/fi2ma.ini finmarc.xml MARCXML.xml
7. Lisää Koha to Mapping biblioitems kohtaan collectiontitle:lle 035 field
8. perl kohaclone/misc/migration_tools/OrigoMigration/bulkmarcimport.pl -b -s -commit 1000 -m MARCXML -file OrigoMMTPerl/OrigoComplete/MARC21.xml
9. add_items.pl - Ennen ajoa tarkista: library_change, location_change, ccode_change, subloc_change(ei ole kaikilla käytössä), notforloan_change, itype_change.
10. add_issues.pl - Ennen ajoa tarkista: library_change.
11. add_accountlines.pl - Ennen ajoa tarkista: type_change.
12. add_reserves.pl - Ennen ajoa tarkista: library_change
13. fix_reserve_rankings.pl
14. add_aqorders.pl - Ennen ajoa tarkista: library_change, itype_change.
15. component_part_repairs_itypes.pl - Ennen ajoa tarkista: addEbook, fixComponentPartLeader, fixRecordLeader ja hae kannasta vain uusimmat muokkausta tarvitsevat biblioitems rivit, eli muokkaa alun sql-lausetta.
16. kohaclone/misc/cronjobs/update_biblio_data_elements.pl -v 2 -f
