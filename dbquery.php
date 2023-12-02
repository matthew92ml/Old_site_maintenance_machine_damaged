<?php
$mode = isset($_GET['mode']) ? $_GET['mode'] : null;
    //tabella "universale"
    if (!@$dbconn) {
        exit;
    }
    switch ($mode) {
        case ('reset'):
        {
            $result = pg_prepare($dbconn, "query", "SELECT NomeStazione , SUM (evento) as Riavvii from (SELECT CONCAT (nome,' ',idstazione) as NomeStazione, 
			                                        COUNT(case when dataora>(ultimadata + interval '30 second') then idevento else '' end ) as evento 
													from infocomune right join eventi on eventi.idcomune=infocomune.id right join (Select id,  LAG(dataora,1) over (order by dataora) 
													as ultimadata from eventi where (idevento LIKE '%AVVIO%' OR idevento LIKE '%RESET%') AND idcomune = $1  AND dataora::date between $2 AND $3 
													AND dataora::time not between '02:59:59' AND '04:00:00' group by id, dataora order by dataora DESC ) as c on  c.id=eventi.id
                                                    where (idevento LIKE '%AVVIO%' or idevento LIKE '%RESET%') AND idcomune = $1 AND dataora::date between $2 AND $3 AND dataora::time not between 
													'02:59:59' AND '04:00:00' group by nome , eventi.id , idcomune, idstazione, dataora, ultimadata) as event group by nomestazione;");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
	    case ('cronologiastazione'):
        {
            $results = pg_prepare($dbconn, "query", "Select distinct note as NumeroEvento , idevento as TipoEvento, dataora as Data from eventi
                                              where (idevento LIKE '%AVVIO%' or idevento LIKE '%RESET%') AND idcomune =$1 AND idstazione = $2 AND dataora::date between $3 AND $4 AND 
					                          dataora::time not between '02:59:59' AND '04:00:00' order by dataora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine));
            break;
        }
        case ('calsvuotamenti'):
        {
            $query = "SELECT * FROM cal_svuotamenti";
            $results = pg_query($query);
            break;
        }
	    case ('doppiconferimenti'):
        {
            $results = pg_prepare($dbconn, "query", "select distinct nome, codicestazione, codicetag, dataora, peso, stringaid, count AS Conteggio from infocomune
                                  inner join (Select distinct idcomune , codicestazione, codicetag as codicetag, dataora, peso, tiporifiuto,
                                  string_agg(id::text , ' ') AS stringaid, count(id) as count from conferimenti where dataora>=$1
                                  group by codicetag, idcomune, codicestazione, peso, dataora, tiporifiuto order by dataora DESC) as tot
                                  on tot.idcomune=infocomune.id where count>1 group by nome, codicetag, codicestazione, dataora, peso, stringaid, count order by dataora DESC");
            $results = pg_execute($dbconn, "query", array($inizio));
            break;
        }
		case ('pesibloccati'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT * from ( SELECT CONCAT (nome,' ',stazione) as identificativo, peso, COUNT( peso) as numeroconferimenti, max(dataora) as ultimodato from
                                                     (SELECT conferimenti.idcomune, conferimenti.codicestazione as stazione, peso, CASE WHEN peso=pp THEN 1 ELSE 0 END as verifica, conferimenti.dataora
                                                     from conferimenti LEFT join (SELECT  idcomune, codicestazione, lag(peso, 1) over (order by idcomune, codicestazione, dataora) as pp, dataora
                                                     from conferimenti ) as last on last.idcomune= conferimenti.idcomune AND last.codicestazione = conferimenti.codicestazione
                                                     AND last.dataora = conferimenti.dataora where conferimenti.dataora::date >= $1::date- interval '2 day'
                                                     group by conferimenti.idcomune, conferimenti.codicestazione, peso, pp, conferimenti.dataora) AS dat INNER JOIN infocomune
                                                     on  infocomune.id = dat.idcomune where verifica=1 group by dat.idcomune, nome, stazione, peso, verifica) AS dat where numeroconferimenti>3");
            $results = pg_execute($dbconn, "query", array($inizio));
            break;
        }
        case ('testcrosstab'):
        {
            $query = "SELECT * FROM cal_test";
            $results = pg_query($query);
            break;
        }
        case ('allarmi'):
            $results = pg_prepare($dbconn, "query", "Select nome, idstazione, (case when Count(terminate)>Count(inceppate) then 'ETICHETTE TERMINATE' else 'ETICHETTE INCEPPATE' END)
                                                     as Avviso,(case when Count(terminate)>Count(inceppate) then SUM(terminate) else SUM(inceppate) END)as Segnalazioni,
													 CONCAT (CASE TO_CHAR(max(ultimadata::date), 'Dy') when 'Sun' then 'Domenica' when 'Mon' then 'Lunedì' when 'Tue' then 'Marterdì' 
                                                     when 'Wed' then 'Mercoledì' when 'Thu' then 'Giovedì' when 'Fri' then 'Venerdì' when 'Sat' then 'Sabato' END, ' ' ,max(ultimadata::date)) AS Ultimoinvio
                                                     from (Select idcomune, idstazione, count( case idevento when 'ETICHETTE TERMINATE' then idevento else NULL END) as terminate, 
                                                     count( case idevento when 'ETICHETTE INCEPPATE' then idevento else NULL END) as inceppate , ultimadata from eventi INNER JOIN
                                                     (Select id,  LAG(dataora,1) over (order by dataora) as ultimadata from eventi where idevento LIKE '%ETICHETTE%'
                                                     AND dataora BETWEEN $1 AND $2::timestamp + interval '1 day') as data on eventi.id = data.id where ultimadata 
													 IS NOT NULL AND idevento ILIKE '%ETICHETTE%' group by idcomune, idstazione, ultimadata)as conteggio INNER JOIN infocomune 
                                                     on conteggio.idcomune = infocomune.id group by nome, idstazione;");
            $results = pg_execute($dbconn, "query", array($inizio, $fine));
			break;
	    case ('tensione'):
            $results = pg_prepare($dbconn, "query", "Select nome, stazione, evento, CASE TO_CHAR(data1, 'Dy') when 'Sun' then 'Domenica' when 'Mon' then 'Lunedì' when 'Tue' then 'Marterdì' when 'Wed' 
                                                     then 'Mercoledì' when 'Thu' then 'Giovedì' when 'Fri' then 'Venerdì' when 'Sat' then 'Sabato' END AS Giorno, data1 As Orario from
                                                     (Select distinct eventi.idcomune as Comune, idstazione as Stazione, CONCAT (idevento, ' ', note) as evento, dataora as data1, LAG(dataora,1 )
   													 over (order by idstazione, dataora) as data2, CONCAT (idevento, ' ', LAG(note,1 ) over (order by idstazione, dataora)) as evento2
                                                     from eventi inner join infostazioni on infostazioni.idcomune=eventi.idcomune AND  infostazioni.codicestazione=eventi.idstazione
                                                     where idevento LIKE '%TENSIONE_RETE%' AND eventi.idcomune = $1 AND ".(($stazione==!null)?"eventi.idstazione = $2 AND dataora::date BETWEEN $3 AND $4 ":"
													 dataora::date BETWEEN $2 AND $3 ")."AND dataora::time NOT BETWEEN '02:50:00' AND '03:50:00' group by  eventi.idcomune, idstazione, idevento, note, dataora
													 order by idstazione, dataora DESC) as operation inner join infocomune on operation.comune=infocomune.id where evento != evento2 
                                                     group by nome, stazione , evento, data1 order by stazione, data1 DESC");
            $results = ($stazione==!null)?pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine)):pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        case ('tensione2'):
        if ($stazione==!null){
            //$results = pg_prepare($dbconn, "query", "SELECT idevento, dataora::date AS data, dataora::time AS ora, note FROM eventi WHERE idevento = 'TENSIONE_RETE' AND idcomune = $1 AND idstazione = $2 AND dataora::date BETWEEN $3 AND $4 ORDER BY dataora DESC");
            $results = pg_prepare($dbconn, "query", "SELECT a.idevento, a.dataora, a.note FROM eventi AS a WHERE a.dataora::date BETWEEN $3 AND $4 AND a.idevento = 'TENSIONE_RETE' AND idcomune = $1 AND idstazione = $2 AND a.note <> ( SELECT b.note FROM eventi AS b WHERE a.idcomune = b.idcomune AND a.idstazione = b.idstazione AND a.dataora > b.dataora ORDER BY b.dataora DESC LIMIT 1 ) ORDER BY a.dataora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine));
            break;
        }
        else {
            //$results = pg_prepare($dbconn, "query", "SELECT idevento, dataora::date AS data, dataora::time AS ora, idstazione AS stazione, note FROM eventi WHERE idevento = 'TENSIONE_RETE' AND idcomune = $1 AND dataora::date BETWEEN $2 AND $3 ORDER BY dataora DESC");
            $results = pg_prepare($dbconn, "query", "SELECT a.idevento, a.dataora, a.idstazione, a.note FROM eventi AS a WHERE a.dataora::date BETWEEN $2 AND $3 AND a.idevento = 'TENSIONE_RETE' AND idcomune = $1 AND a.note <> ( SELECT b.note FROM eventi AS b WHERE a.idcomune = b.idcomune AND a.idstazione = b.idstazione AND a.dataora > b.dataora ORDER BY b.dataora DESC LIMIT 1 ) ORDER BY a.dataora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
	    case ('datinuclei'):
        {
            $results = pg_prepare($dbconn, "query", "select distinct codicenucleo from datinuclei where idcomune=$1 
			AND codicenucleo NOT IN (Select distinct codicenucleo from datiform where idcomune=$1)
            order by codicenucleo");
            $results = pg_execute($dbconn, "query", array($comune)); 
            break;
        }
        case ('allarmetensione'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT idcomune, idstazione, note AS stato, MAX(dataora) AS ultimavolta FROM eventi WHERE idcomune > 0 AND idcomune < 7 AND idevento = 'TENSIONE_RETE'
            AND (idcomune, idstazione) NOT IN (SELECT idcomune, idstazione FROM eventi WHERE dataora::date BETWEEN $1 AND $2 GROUP BY idcomune, idstazione) GROUP BY idcomune, idstazione, note
            ORDER BY idcomune, idstazione");
            $results = pg_execute($dbconn, "query", array($comune, $fine)); 
            break;
        }
        case ('carica'):
            $results = pg_prepare($dbconn, "query", "Select nome, idstazione, CASE TO_CHAR(data, 'Dy') when 'Sun' then 'Domenica' when 'Mon' then 'Lunedi' when 'Tue' then 'Marterdi' when 'Wed' 
                                                     then 'Mercoledi' when 'Thu' then 'Giovedi' when 'Fri' then 'Venerdi' when 'Sat' then 'Sabato' END AS Giorno, case evento  when 'TENSIONE_RETE OFF' 
                                                     then CONCAT (date_part('hour',data- lag(data, 1) OVER (ORDER BY data)),' ORE DI CARICA') when 'TENSIONE_RETE ON' 
                                                     then CONCAT (date_part('hour',data- lag(data, 1) OVER (ORDER BY data)),' ORE DI SCARICA') END AS Batteria, case evento  when 'TENSIONE_RETE ON' 
                                                     then (row_number() OVER() ) - div(row_number() OVER() ,2) when 'TENSIONE_RETE OFF' then NULL END AS Ciclo ,
													 data::date from (Select eventi.idcomune, eventi.idstazione, 
                                                     CONCAT (eventi.idevento, ' ', eventi.note) as evento, eventi.dataora as data, ultimadata, evento2 from eventi inner join 
                                                     (Select id, LAG(CONCAT (idevento, ' ', note),1) over (order by idevento) as evento2, LAG(dataora,1) over (order by dataora) as ultimadata,
													 idcomune, idstazione from eventi where (idevento LIKE '%TENSIONE_RETE%' OR idevento LIKE '%AVVIO%') AND idcomune =$1 AND idstazione=$2 ) as eventi1
                                                     on eventi.id=eventi1.id where eventi.idevento LIKE '%TENSIONE_RETE%' AND eventi.idcomune = $1 AND eventi.idstazione= $2 
                                                     AND eventi.dataora::time NOT BETWEEN ultimadata::time AND (ultimadata::timestamp + interval '50 seconds')::time) as operation
                                                     inner join infocomune on operation.idcomune = infocomune.id  WHERE evento!=evento2 AND CAST(data AS text) NOT LIKE '%2016-10%'
                                                     GROUP BY nome, idstazione, data, evento ORDER BY data DESC;");
            $results = pg_execute($dbconn, "query", array($comune, $stazione));
            break;
        case ('operatori'):
            $results = pg_prepare($dbconn, "query", "SELECT infocomune.nome, codicestazione AS stazione, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Domenica' when 'Mon' then 'Lunedì' when 'Tue' 
                                                     then 'Marterdì' when 'Wed' then 'Mercoledì' when 'Thu' then 'Giovedì' when 'Fri' then 'Venerdì' when 
                                                     'Sat' then 'Sabato' END AS Giorno, dataora AS DATA, infooperatori.codicetag, infooperatori.nome AS operatore, 
                                                     INFO FROM infocomune LEFT JOIN logoperatore ON logoperatore.idcomune=infocomune.id LEFT JOIN infooperatori ON 
													 infooperatori.codicetag=logoperatore.codicetag WHERE logoperatore.idcomune=$1 "
													 .(($stazione==!null)?"AND codicestazione = $2 AND dataora BETWEEN $3 AND $4 ":"AND dataora BETWEEN $2 AND $3 ")." 
                                                     ORDER BY codicestazione ASC, dataora DESC;");
            $results = ($stazione==!null)?pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine)):pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        case ('operatorifiltrati'):
            $results = pg_prepare($dbconn, "query", "SELECT infocomune.nome, codicestazione AS stazione, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Domenica' when 'Mon' then 'Lunedì' when 'Tue' 
                                                     then 'Marterdì' when 'Wed' then 'Mercoledì' when 'Thu' then 'Giovedì' when 'Fri' then 'Venerdì' when 
                                                     'Sat' then 'Sabato' END AS Giorno, dataora AS DATA, infooperatori.codicetag, infooperatori.nome AS operatore, 
                                                     INFO FROM infocomune LEFT JOIN logoperatore ON logoperatore.idcomune=infocomune.id LEFT JOIN infooperatori ON 
													 infooperatori.codicetag=logoperatore.codicetag WHERE logoperatore.idcomune=$1 "
													 .(($stazione==!null)?"AND codicestazione = $2 AND dataora BETWEEN $3 AND $4 ":" AND dataora BETWEEN $2 AND $3 ")."
													 AND info NOT LIKE '%timeout%' AND info NOT LIKE '%logout%' AND info NOT LIKE '%login%'
                                                     ORDER BY codicestazione ASC, dataora DESC;");
            $results = ($stazione==!null)?pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine)):pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        case ('conferimenti'):
            $results = pg_prepare($dbconn, "query", "SELECT distinct nome AS Comune, codicestazione AS Stazione, codicetag, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Domenica' when 'Mon' then 'Lunedi' when 'Tue' then 'Marterdi' when 'Wed' 
                                                     then 'Mercoledi' when 'Thu' then 'Giovedi' when 'Fri' then 'Venerdi' when 'Sat' then 'Sabato' END || ' ' || TO_CHAR(dataora,' YYYY-MM-DD hh24:mm:ss') AS data, peso, tiporifiuto, descrizione,
													 codiceanomalia FROM infocomune JOIN conferimenti c on infocomune.id=c.idcomune  JOIN inforifiuti ON tiporifiuto=codicerifiuto AND inforifiuti.idcomune = c.idcomune WHERE c.idcomune = $1 AND "
													 .(($stazione==!null)?"codicestazione = $2 AND dataora::date BETWEEN $3 AND $4 ":"dataora::date BETWEEN $2 AND $3 ")." GROUP BY nome, codicestazione, codicetag, peso, tiporifiuto, descrizione,
													 codiceanomalia, dataora ORDER BY data DESC;");
            $results =($stazione==!null)?pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine)):pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        case ('letturerror'):
            $results = pg_prepare($dbconn, "query", "Select distinct nome as Comune, idstazione as NumeroStazione, count(codicetag) as numerofallimenti, string_agg(codicetag::text , ' ') 
                                                     AS letturefallite, max(dataora) as ultimodatoricevuto from logfalliti left join infocomune on infocomune.id=logfalliti.idcomune 
                                                     where idcomune=$1 AND dataora>=$2 group by nome, idstazione;");
            $results = pg_execute($dbconn, "query", array($comune, $inizio));
            break;
       case ('multipli'):
        if ($stazione==!null){
            $results = pg_prepare($dbconn, "query", "SELECT codicetag, dataora::date AS data, dataora::time AS ora, tiporifiuto, flag, abs(pesop) AS diffPeso, codiceanomalia, codicestazione
            FROM (SELECT codicetag, LAG(codicetag) OVER (ORDER BY dataora ASC) AS codicep, codiceanomalia, dataora, idcomune, codicestazione, peso - LAG(peso) OVER(ORDER BY dataora ASC) AS pesop, 
			tiporifiuto, tiporifiuto - LAG(tiporifiuto) OVER(ORDER BY dataora ASC) AS flag FROM conferimenti WHERE dataora > '2016-01-10 00:00:00' ORDER BY dataora ASC) AS conferimentp
            WHERE idcomune = $1 AND codicestazione = $2 AND dataora::date BETWEEN $3 AND $4 AND flag = 0 AND codicetag = codicep AND abs(pesop)<1 ORDER BY dataora DESC" );
            $results = pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine));
            break;
        }
        else {
            $results = pg_prepare($dbconn, "query", "SELECT codicetag, dataora::date AS data, dataora::time AS ora, tiporifiuto, flag, abs(pesop) AS diffPeso, codiceanomalia, codicestazione
            FROM (SELECT codicetag, LAG(codicetag) OVER (ORDER BY dataora ASC) AS codicep, codiceanomalia, dataora, idcomune, codicestazione, peso - LAG(peso) OVER(ORDER BY dataora ASC) AS pesop, tiporifiuto, tiporifiuto - LAG(tiporifiuto) OVER(ORDER BY dataora ASC) AS flag
            FROM conferimenti WHERE dataora > '2016-01-10 00:00:00' ORDER BY dataora ASC) AS conferimentp
            WHERE idcomune = $1 AND dataora::date BETWEEN $2 AND $3 AND flag = 0 AND codicetag = codicep AND abs(pesop)<1 ORDER BY dataora DESC" );
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        case ('conferimentiassociati'):
        if ($stazione==!null){
            $results = pg_prepare($dbconn, "query", "SELECT c.codicetag, codicenucleo, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Dom' when 'Mon' then 'Lun' when 'Tue' then 'Mar' when 'Wed' then 'Mer' when 'Thu' then 'Gio' when 'Fri' then 'Ven' when 'Sat' then 'Sab' END AS GG, dataora::date AS data, dataora::time AS ora, descrizione, peso, tiporifiuto, codiceanomalia FROM conferimenti c JOIN inforifiuti ON tiporifiuto=codicerifiuto AND inforifiuti.idcomune = c.idcomune LEFT JOIN associazionenuclei ON c.codicetag = associazionenuclei.codicetag WHERE c.idcomune = $1 AND codicestazione = $2 AND dataora::date BETWEEN $3 AND $4 ORDER BY data, ora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine));
            break;
        }
        else {
            $results = pg_prepare($dbconn, "query", "SELECT codicestazione AS stazione, c.codicetag, codicenucleo, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Dom' when 'Mon' then 'Lun' when 'Tue' then 'Mar' when 'Wed' then 'Mer' when 'Thu' then 'Gio' when 'Fri' then 'Ven' when 'Sat' then 'Sab' END AS GG, dataora::date AS data, dataora::time AS ora, descrizione, peso, tiporifiuto, codiceanomalia FROM conferimenti c JOIN inforifiuti ON tiporifiuto=codicerifiuto AND inforifiuti.idcomune = c.idcomune LEFT JOIN associazionenuclei ON c.codicetag = associazionenuclei.codicetag WHERE c.idcomune = $1 AND dataora::date BETWEEN $2 AND $3 ORDER BY dataora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
		case ('cassonetti'):
            $results = pg_prepare($dbconn, "query", "SELECT * FROM ( SELECT CONCAT( nome , ' ' , stazione) AS identificativo, CONCAT( 'Cassonetto ', descrizione) AS rifiuto, COUNT (CASE WHEN 
			                                         anomalia='ANOMALIA' THEN anomalia ELSE NULL END ) as numeroanomalie from inforifiuti inner join (SELECT DISTINCT ON (dataora) comune, stazione, dataora,
													 tiporifiuto, codiceanomalia, input, evento, data, data2, evento2 ,(CASE WHEN evento2 NOT ILIKE '%' || CAST( input as text) || '%' THEN 'ANOMALIA'
                                                     ELSE 'OK' END ) as Anomalia from (SELECT distinct dati.idcomune , dati.codicestazione , dati.tiporifiuto, dati.dataora, codiceanomalia,
                                                     inputplc1 as input from (SELECT distinct idcomune, codicestazione, tiporifiuto, codiceanomalia, dataora from conferimenti
                                                     where dataora BETWEEN $1 AND $2) AS dati inner join infocassonetti AS info on info.idcomune=dati.idcomune
													 AND info.codicestazione = dati.codicestazione AND info.tiporifiuto = dati.tiporifiuto) AS dati inner join
													 (Select DISTINCT ON (data, data2) Comune, Stazione, evento, data, data2, evento2, inputplc1 from infocassonetti
                                                     inner join ( Select distinct eventi.idcomune as Comune, eventi.idstazione as Stazione, CONCAT (idevento, ' ',
                                                     (CASE WHEN (octet_length(note) BETWEEN 1 AND 3) THEN note END)) as evento, dataora as data, data2, evento2 , note, note2
                                                     from eventi INNER JOIN ( SELECT idcomune, idstazione, LAG( dataora,1 ) over (order by idcomune, idstazione, dataora) as data2,
                                                     CONCAT (idevento, ' ', LAG(((CASE WHEN  (octet_length(note) BETWEEN 1 AND 3) THEN note END)),1 ) 
													 over (order by idcomune, idstazione,  dataora)) as evento2, LAG( note,1 ) over (order by idcomune, idstazione,  dataora) as note2
                                                     from eventi where idevento ='CHIUSURA' group by idcomune, idstazione, idevento, note, dataora order by idcomune, idstazione, dataora DESC)
													 as eventi2 on eventi2.idcomune=eventi.idcomune AND eventi2.idstazione=eventi.idstazione where idevento = 'APERTURA' AND dataora::date BETWEEN 
                                                     $1 AND $2 AND data2 BETWEEN dataora AND dataora + interval '2 minute' group by  eventi.idcomune, eventi.idstazione, 
                                                     idevento, note, dataora, data2, evento2, note2 order by eventi.idcomune, eventi.idstazione, dataora DESC) as event on
                                                     event.comune=infocassonetti.idcomune AND event.stazione = infocassonetti.codicestazione AND (CAST((CASE WHEN  octet_length(event.note) 
                                                     BETWEEN 1 AND 3 THEN event.note END) as smallint) = infocassonetti.inputplc1 OR CAST((CASE WHEN  octet_length(event.note2) 
													 BETWEEN 1 AND 3 THEN event.note2 END) as smallint) = infocassonetti.inputplc1)) as event2 on dati.idcomune = event2.comune 
                                                     AND dati.codicestazione = event2.stazione AND dati.input = event2.inputplc1 where data2 BETWEEN dataora AND dataora + interval '2 minute'
                                                     AND data BETWEEN dataora - interval '10 second' AND dataora) as info ON info.comune = inforifiuti.idcomune  AND 
													 info.tiporifiuto = inforifiuti.codicerifiuto inner join infocomune ON info.comune = infocomune.id group by nome, stazione, descrizione)AS filter
                                                     where numeroanomalie != 0 ;");
            $results = pg_execute($dbconn, "query", array( $inizio, $fine));
            break;
        case ('conferimentirecenti'):
        if ($stazione==!null){
            $results = pg_prepare($dbconn, "query", "SELECT idcomune, COUNT(id) as numconferimenti, MAX(dataora) as ultimaora,
                    Sum(CASE WHEN tiporifiuto = 0 THEN peso END) indifferenziato,
                    Sum(CASE WHEN tiporifiuto = 17 THEN peso END) pannolini,
                    Sum(CASE WHEN tiporifiuto = 16 THEN peso END) barattolame,
                    Sum(CASE WHEN tiporifiuto = 3 THEN peso END) plastica,
                    Sum(CASE WHEN tiporifiuto = 1 THEN peso END) carta,
                    Sum(CASE WHEN tiporifiuto = 15 THEN peso END) vetro,
                    Sum(CASE WHEN tiporifiuto = 2 THEN peso END) vetroelatta,
                    Sum(CASE WHEN tiporifiuto = 4 THEN peso END) organico,
                    Sum(CASE WHEN tiporifiuto > 0 THEN peso END) diff, SUM(peso) as totpeso
                    FROM conferimenti WHERE idcomune = $1 AND codicestazione=$2 AND dataora::date BETWEEN $3 AND $4 GROUP BY idcomune");
            $results = pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine));
            break;
        }
        else {
            $results = pg_prepare($dbconn, "query", "SELECT idcomune, codicestazione, COUNT(id) as numconferimenti, MAX(dataora) as ultimaora,
                    Sum(CASE WHEN tiporifiuto = 0 THEN peso END) indifferenziato,
                    Sum(CASE WHEN tiporifiuto = 17 THEN peso END) pannolini,
                    Sum(CASE WHEN tiporifiuto = 16 THEN peso END) barattolame,
                    Sum(CASE WHEN tiporifiuto = 3 THEN peso END) plastica,
                    Sum(CASE WHEN tiporifiuto = 1 THEN peso END) carta,
                    Sum(CASE WHEN tiporifiuto = 15 THEN peso END) vetro,
                    Sum(CASE WHEN tiporifiuto = 2 THEN peso END) vetroelatta,
                    Sum(CASE WHEN tiporifiuto = 4 THEN peso END) organico,
                    Sum(CASE WHEN tiporifiuto > 0 THEN peso END) diff, SUM(peso) as totpeso
                    FROM conferimenti WHERE idcomune = $1 AND dataora::date BETWEEN $2 AND $3 GROUP BY idcomune, codicestazione");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        case ('perstazione'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT DISTINCT(codicetag), idcomune, codicestazione, COUNT(*) AS conferimenti, SUM(peso) AS totpeso, MAX(dataora) AS ultimo FROM conferimenti WHERE dataora::date BETWEEN $1 AND $2 GROUP BY idcomune, codicestazione, codicetag ORDER BY codicetag;");
            $results = pg_execute($dbconn, "query", array($inizio, $fine));
            break;
        }
        case ('pesialti'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT c.codicestazione, c.codicetag, codicenucleo, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Dom' when 'Mon' then 'Lun' when 'Tue' then 'Mar' when 'Wed' then 'Mer' when 'Thu' then 'Gio' when 'Fri' then 'Ven' when 'Sat' then 'Sab' END AS GG, dataora::date AS data, dataora::time AS ora, descrizione, peso, tiporifiuto, codiceanomalia FROM conferimenti c JOIN inforifiuti ON tiporifiuto=codicerifiuto AND inforifiuti.idcomune = c.idcomune LEFT JOIN associazionenuclei ON c.codicetag = associazionenuclei.codicetag WHERE c.idcomune = $1 AND dataora::date BETWEEN $2 AND $3 AND peso >= $4 ORDER BY data, ora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine, $peso));
            break;
        }
        case ('pesizero'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT idcomune, codicestazione, codicetag, dataora, peso, tiporifiuto, nome, codiceanomalia, data, componenti FROM conferimenti JOIN (SELECT DISTINCT(codicerifiuto), MIN(descrizione) AS nome FROM inforifiuti GROUP BY codicerifiuto) AS rifiuti ON tiporifiuto=codicerifiuto WHERE dataora::date BETWEEN $1 AND $2 AND peso = 0 ORDER BY dataora DESC;");
            $results = pg_execute($dbconn, "query", array($inizio, $fine));
            break;
        }
        case ('conferimentiassociati'):
        if ($stazione==!null){
            $results = pg_prepare($dbconn, "query", "SELECT c.codicetag, codicenucleo, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Dom' when 'Mon' then 'Lun' when 'Tue' then 'Mar' when 'Wed' then 'Mer' when 'Thu' then 'Gio' when 'Fri' then 'Ven' when 'Sat' then 'Sab' END AS GG, dataora::date AS data, dataora::time AS ora, descrizione, peso, tiporifiuto, codiceanomalia FROM conferimenti c JOIN inforifiuti ON tiporifiuto=codicerifiuto AND inforifiuti.idcomune = c.idcomune LEFT JOIN associazionenuclei ON c.codicetag = associazionenuclei.codicetag WHERE c.idcomune = $1 AND codicestazione = $2 AND dataora::date BETWEEN $3 AND $4 ORDER BY data, ora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine));
            break;
        }
        else {
            $results = pg_prepare($dbconn, "query", "SELECT codicestazione AS stazione, c.codicetag, codicenucleo, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Dom' when 'Mon' then 'Lun' when 'Tue' then 'Mar' when 'Wed' then 'Mer' when 'Thu' then 'Gio' when 'Fri' then 'Ven' when 'Sat' then 'Sab' END AS GG, dataora::date AS data, dataora::time AS ora, descrizione, peso, tiporifiuto, codiceanomalia FROM conferimenti c JOIN inforifiuti ON tiporifiuto=codicerifiuto AND inforifiuti.idcomune = c.idcomune LEFT JOIN associazionenuclei ON c.codicetag = associazionenuclei.codicetag WHERE c.idcomune = $1 AND dataora::date BETWEEN $2 AND $3 ORDER BY dataora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        case ('logAdminTester'):{
            $results = pg_prepare($dbconn, "query", "Select infocomune.nome, codicestazione, lat1.nome as Descrizione, logadmintesters.tipologiautente as tipologia, 
                                                     logadmintesters.dataora as data, lat1.codicetag as codicetessera from (Select codicetag, nome from infotesters 
                                                     UNION ALL select codicetag, nome from infoamministratori) as lat1 left join logadmintesters on logadmintesters.codicetag=
                                                     lat1.codicetag inner join infocomune on logadmintesters.idcomune=infocomune.id where idcomune = $1 "
													 .(($stazione==!null)?"AND codicestazione = $2 AND dataora BETWEEN $3 AND $4":"AND dataora BETWEEN $2 AND $3")." 
													 order by  logadmintesters.dataora DESC;");
            $results = ($stazione==!null)?pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine)):pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        case ('zeroconferimenti'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT a.codicenucleo, a.idcomune, lotto, string_agg(distinct(a.codicetag), ', ') AS codicitag, string_agg(distinct(i.progressivo::text), ', ') AS progressivi,
                Coalesce(count(c.id), 0) AS conferimenti, min(primo) AS primo, max(ultimo) AS ultimo, now()::date - max(ultimo::date) AS giorni, attivato, info,
                 trim(cognome) AS cognome, trim(nome) AS nome, d.indirizzo, d.telefono, d.cellulare, d.mail
                FROM associazionenuclei a
                LEFT JOIN conferimenti c ON a.codicetag = c.codicetag
                INNER JOIN infonuclei i ON a.codicetag = i.codicetag
                INNER JOIN utentiweb u ON a.id = u.idnucleo
                INNER JOIN datinuclei d ON a.codicenucleo = d.codicenucleo AND a.idcomune = d.idcomune
                LEFT JOIN (SELECT COUNT(*) AS conteggio, codicetag, min(dataora) AS primo, max(dataora) AS ultimo FROM conferimenti
                WHERE idcomune = $1 GROUP BY codicetag) AS j ON a.codicetag = j.codicetag
                LEFT JOIN datiform df ON a.codicenucleo = df.codicenucleo AND a.idcomune = df.idcomune
                WHERE a.idcomune = $1 AND i.idcomune = $1  AND a.codicetag NOT IN (SELECT codicetag FROM conferimenti c WHERE idcomune = $1 AND dataora::date BETWEEN $2 AND $3)
                GROUP BY df.lotto, a.codicenucleo, a.idcomune, attivato, info, cognome, nome, d.indirizzo, d.telefono, d.cellulare, d.mail;");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        case ('zeroconferimentiminimale'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT a.codicenucleo, a.idcomune, string_agg(distinct(a.codicetag), ', ') AS codicitag,
                Coalesce(count(c.id), 0) AS conferimenti
                FROM associazionenuclei a
                LEFT JOIN conferimenti c ON a.codicetag = c.codicetag AND dataora::date BETWEEN $2 AND $3
                WHERE a.idcomune = $1
                GROUP BY a.idcomune, a.codicenucleo
                HAVING Coalesce(count(c.id), 0) = 0");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        case ('coperchi'):
        if ($stazione==!null){
            $results = pg_prepare($dbconn, "query", "SELECT idevento, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Dom' when 'Mon' then 'Lun' when 'Tue' then 'Mar' when 'Wed' then 'Mer' when 'Thu' then 'Gio' when 'Fri' then 'Ven' when 'Sat' then 'Sab' END AS GG, dataora::date AS data, dataora::time AS ora, note FROM eventi WHERE idevento = 'APERTURA' AND idcomune = $1 AND idstazione = $2 AND dataora::date BETWEEN $3 AND $4 ORDER BY dataora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine));
            break;
        }
        else {
            $results = pg_prepare($dbconn, "query", "SELECT idevento, CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Dom' when 'Mon' then 'Lun' when 'Tue' then 'Mar' when 'Wed' then 'Mer' when 'Thu' then 'Gio' when 'Fri' then 'Ven' when 'Sat' then 'Sab' END AS GG, dataora::date AS data, dataora::time AS ora, idstazione AS stazione, note FROM eventi WHERE idevento = 'APERTURA' AND idcomune = $1 AND dataora::date BETWEEN $2 AND $3 ORDER BY dataora DESC");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        case ('etichette'):
        {
            $query = " SELECT Nome As Comune, idstazione AS stazione,  PrimaSettimana, SecondaSettimana, case when div(numeroetichette,maxetichette) < 1  then maxetichette-numeroetichette else
			           (maxetichette - (numeroetichette-div(numeroetichette,maxetichette)*maxetichette)) end AS restanti, case when div(numeroetichette,maxetichette) < 1  then numeroetichette else                           
			           (numeroetichette-div(numeroetichette,maxetichette)*maxetichette) end  AS stampate, maxetichette FROM infocomune inner join datistazione on datistazione.idcomune=infocomune.id
                        inner join ( Select distinct idcomune, codicestazione, count(peso) AS PrimaSettimana from conferimenti where dataora::date BETWEEN  now()::date-interval'7 days'
                        AND CURRENT_DATE group by idcomune, codicestazione order by idcomune, codicestazione ) as seven on seven.idcomune=datistazione.idcomune
                        AND seven.codicestazione=datistazione.idstazione inner join ( Select distinct idcomune, codicestazione, count(peso) AS SecondaSettimana from conferimenti
                        where dataora::date BETWEEN now()::date-interval'14 days' AND now()::date-interval'7 days' group by idcomune, codicestazione order by idcomune, codicestazione) as fourteen 
                        on fourteen.idcomune=seven.idcomune AND fourteen.codicestazione=seven.codicestazione WHERE datistazione.idcomune > 0 AND idstazione > 0 ORDER BY datistazione.idcomune, idstazione ASC";
            $results = pg_query($query);
            break;
        }
        case ('info'):
        {
            $query = "SELECT infocomune.nome as Comune , idstazione AS NumeroStazione, (case secondari when 0 then 'NO' when 1 then 'SI' END) as rifiutisecondari, versione, 
			          ipvpn as IndirizzoVpn , infostazioni.nome , tipo as alimentazione, controllerplc AS PLC, (case voltometer485 when 0 then 'NO' when 1 then 'SI' END) 
					  AS voltometro485, router, stampante, (case scontrini when 0 then 'NO' when 1 then 'SI' END) AS scontrini, (case gps when 0 then 'NO' when 1 then 'SI' END) AS GPS,
					  visore AS visore_bilancia, dvr AS sorveglianza , (case sbarra when 0 then 'NO' when 1 then 'SI' END) AS sbarra FROM infocomune INNER JOIN datistazione on 
					  datistazione.idcomune=infocomune.id INNER JOIN infostazioni on infostazioni.idcomune=datistazione.idcomune AND 
					  infostazioni.codicestazione=datistazione.idstazione INNER JOIN alimentazione ON infostazioni.alimentazione=alimentazione.id ORDER BY infocomune.nome, idstazione ASC;";
            $results = pg_query($query);
            break;
        }
        case ('infostazioni'):
        {
            $query = "SELECT idcomune, codicestazione AS stazione, nome, tipo as alimentazione FROM infostazioni INNER JOIN alimentazione ON alimentazione=alimentazione.id WHERE idcomune > 0 AND codicestazione > 0 ORDER BY idcomune, codicestazione ASC";
            $results = pg_query($query);
            break;
        }
        case ('cumuli'):
        if ($stazione==!null){
            $results = pg_prepare($dbconn, "query", "SELECT CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Dom' when 'Mon' then 'Lun' when 'Tue' then 'Mar' when 'Wed' then 'Mer' when 'Thu' then 'Gio' when 'Fri' then 'Ven' when 'Sat' then 'Sab' END AS GG, dataora::date AS data, dataora::time AS ora, codicestazione, idrifiuto, nomerifiuto, peso, numerosacchetti, idbidone
            FROM cumulopesi WHERE idcomune = $1 AND codicestazione = $2 AND dataora::date BETWEEN $3 AND $4 ORDER BY dataora DESC" );
            $results = pg_execute($dbconn, "query", array($comune, $stazione, $inizio, $fine));
            break;
        }
        else {
        $results = pg_prepare($dbconn, "query", "SELECT CASE TO_CHAR(dataora, 'Dy') when 'Sun' then 'Dom' when 'Mon' then 'Lun' when 'Tue' then 'Mar' when 'Wed' then 'Mer' when 'Thu' then 'Gio' when 'Fri' then 'Ven' when 'Sat' then 'Sab' END AS GG, dataora::date AS data, dataora::time AS ora, codicestazione, idrifiuto, nomerifiuto, peso, numerosacchetti, idbidone
        FROM cumulopesi WHERE idcomune = $1 AND dataora::date BETWEEN $2 AND $3 ORDER BY dataora DESC");
        $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
        break;
        }
		case ('mediecumuli'):
        {
            $query = "SELECT idcomune, codicestazione, nomerifiuto, CAST(AVG(peso) as decimal(6,3)) as mediapeso, CAST(AVG(numerosacchetti) as decimal(6,3)) as mediasacchetti, MAX(dataora) AS aggiornato from cumulopesi GROUP BY idcomune, codicestazione, nomerifiuto;";
            $results = pg_query($query);
            break;
        }
        case ('tagmultiple'):
        {
            $query = "SELECT COUNT(*) AS ntag, codicenucleo, idcomune FROM associazionenuclei GROUP BY codicenucleo, idcomune HAVING COUNT(codicetag) > 1 ORDER BY codicenucleo;";
            $results = pg_query($query);
            break;
        }
        case ('senzatessera'):
        {
            $query = "SELECT idcomune, cognome, nome, codicenucleo, telefono, cellulare, mail FROM datinuclei WHERE codicenucleo NOT IN (SELECT DISTINCT(codicenucleo) FROM associazionenuclei) ORDER BY cognome;";
            $results = pg_query($query);
            break;
        }
        case ('ultimonucleo'):
        {
            $query = "SELECT id, codicenucleo, cognome, nome, username, idcomune FROM (SELECT id, codicenucleo, cognome, nome, idcomune FROM datinuclei WHERE id IN (SELECT MAX(id) FROM datinuclei WHERE idcomune > 0 GROUP BY idcomune)) t1 INNER JOIN (SELECT username, idnucleo FROM utentiweb) t2 ON t1.id = t2.idnucleo ORDER BY idcomune DESC;";
            $results = pg_query($query);
            break;
        }
        case ('numtag'):
        {
            $query = "SELECT idcomune, COUNT(*) as numtag FROM infonuclei WHERE idcomune > 0 GROUP BY idcomune ORDER BY idcomune;";
            $results = pg_query($query);
            break;
        }
        case ('tagcomuni'):
        {
            $query = "SELECT i.idcomune, i.id, i.codicetag, a.codicenucleo, d.cognome, d.nome, attivata FROM infonuclei i LEFT JOIN associazionenuclei a on i.codicetag = a.codicetag LEFT JOIN datinuclei d ON a.codicenucleo = d.codicenucleo ORDER BY codicetag;";
            $results = pg_query($query);
            break;
        }
        case ('tagassociate'):
        {
            $query = "SELECT codicetag, idcomune, attivata FROM infonuclei ORDER BY idcomune, codicetag;";
            $results = pg_query($query);
            break;
        }
        case ('tagerrate'):
        {
            $query = "SELECT idcomune, codicetag, char_length(codicetag) AS ncar, codicenucleo, info, progressivo FROM associazionenuclei WHERE char_length(codicetag)<>8 AND codicenucleo IN (SELECT codicenucleo FROM datinuclei);";
            $results = pg_query($query);
            break;
        }
        case ('nonassociate'):
        {
            $query = pg_prepare($dbconn, "query", "SELECT infonuclei.* FROM infonuclei WHERE codicetag NOT IN 
			                                       (SELECT codicetag FROM associazionenuclei) 
			                                       AND idnucleo is NULL AND idcomune = $1 order by codicetag;");
			$results = pg_execute($dbconn, "query", array($comune));
            break;
        }
        case ('usernamedoppi'):
        {
            $query = "SELECT COUNT(username) AS ndoppi, username, MIN(idnucleo) primoid, MAX(idnucleo) ultimoid, MIN(ultimoaccesso) primoaccesso, MAX(ultimoaccesso) ultimoaccesso FROM utentiweb GROUP BY username ORDER BY ndoppi DESC;";
            $results = pg_query($query);
            break;
        }
        case ('utentiweb'):
        {
            $query = "SELECT id, username, usertype, idnucleo, attivato, ultimoaccesso FROM utentiweb ORDER BY id ASC;";
            $results = pg_query($query);
            break;
        }
        case ('svuotamenti'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT * FROM cal_logoperatore WHERE idcomune = $1 AND dataora BETWEEN $2 AND $3;");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        case ('nonconferiti'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT a.codicetag, a.tipi, d.num, b.codicenucleo, TRIM(TRAILING FROM c.nome) AS nome, TRIM(TRAILING FROM c.cognome) AS cognome, TRIM(TRAILING FROM c.indirizzo) as indirizzo FROM (SELECT idcomune, codicetag, count(distinct(tiporifiuto)) AS tipi FROM conferimenti WHERE idcomune = $1 AND dataora > '2017-01-01 00:00:00' group by codicetag, idcomune) AS a JOIN associazionenuclei AS b ON a.codicetag=b.codicetag JOIN datinuclei AS c ON b.codicenucleo = c.codicenucleo JOIN (SELECT t.codicetag, t.num FROM (SELECT codicetag, count(*) AS num FROM conferimenti WHERE dataora > '2017-01-01 00:00:00' GROUP BY codicetag) AS t) AS d ON a.codicetag = d.codicetag ORDER BY a.idcomune, tipi;");
            $results = pg_execute($dbconn, "query", array($comune));
            break;
        }
        case ('rdannuale'):
        {
            if ($stazione==!null){
            $results = pg_prepare($dbconn, "query", "SELECT cognome, nome, indirizzo, codicenucleo, sum(indifferenziato) AS indifferenziato, sum(pannolini) AS pannolini, sum(barattolame) AS barattolame, sum(plastica) AS plastica, sum(carta) AS carta, sum(vetro) AS vetro, sum(organico) AS organico, Sum(case when tiporifiuto <> 0 then percent end) AS rd, Count(*) AS coef, Sum(DISTINCT(tot)) AS tot FROM(SELECT cognome, nome, indirizzo, codicenucleo, sum(case when tiporifiuto = 16 then somma end) barattolame, sum(case when tiporifiuto = 3 then somma end) plastica, sum(case when tiporifiuto = 1 then somma end) carta, sum(case when tiporifiuto = 15 then somma end) vetro, sum(case when tiporifiuto = 0 then somma end) indifferenziato, sum(case when tiporifiuto = 17 then somma end) pannolini, sum(case when tiporifiuto = 4 then somma end) organico, tiporifiuto, percent, tot FROM rdannuale WHERE idcomune = $1 AND cognome || ' ' || nome IN ('ACCOLLA ANGELO', 'ACCOLLA CARMELO GIUSEPPE', 'AGOSTINI RENZO', 'AMAOLO EDOARDO', 'ANCONA ALDO', 'ANDRENUCCI DIOMIRA', 'ANGELINI ANDREA', 'ANTONELLI ALESSANDRO', 'ATTIVISSIMO EMANUELE', 'BASTARELLI GIUSEPPE', 'BIANCHINI ALBERTO', 'BORDONI FILOMENA', 'CALLARA'' GIUSEPPINA', 'CAMPOFILONI IVANA', 'CAMPOFILONI LORENZINO', 'CANTATORE MARIA', 'CARDINALI IVANA', 'CARDONE VITTORIO', 'CERASETTI GIUSEPPE', 'CHIOINI PATRIZIA', 'CIARROCCHI MARIA PIA', 'CIARROCCHI TERESA', 'CICCOLA GIUSEPPE', 'CILIBERTI DOMENICO', 'CIUCANI MARIA TERESA', 'CURI LUDOVICO', 'DE ROSSI DANIELE', 'DE SANTIS DALMAZIO', 'DI MARZIO ISIDORO', 'DI SANTO MARIO', 'DIADORO BENEDETTA', 'DONZELLI MARCO', 'DONZELLI MARIELLA', 'DONZELLI MAURELIO', 'EGIDI MARCELLO', 'EVANGELISTI EUGENIO', 'FIORI CLAUDIO', 'GALLI GIAN PIERO', 'GEROSA IRENEO', 'GIACOBBI ADELE', 'GIACOBBI GUERRIERO', 'GIACOBBI LUIGINA', 'GIACOPETTI GIOVANNI', 'GIAMPIERI PATRIZIA', 'GIDIUCCI GIUSEPPE', 'GIORGETTI FRANCESCO', 'IACOPINI ILEANA', 'LEONI GIANLUCA', 'LOMBARDI PAOLO', 'LUCATTELLI GIUSEPPE MARIA', 'LUCCI RENZO', 'MANCINI GIORGIO', 'MARIANI GIOVANNI', 'MARZIALI FRANCO', 'MARZIALI MASSIMILIANO', 'MARZONI MERI', 'MATA'' STEFANIA', 'MATTETTI PAOLA', 'MATTIOLI LUCIANA', 'MAURIZI CARLA', 'MENGONI MARIANO', 'MIHAILA GHEORGHE MIHAI', 'MINNUCCI DEANNA', 'MINNUCCI MARIA PIA', 'MONINA ELISABETTA', 'MONTELPARE GRAZIELLA', 'NAZZICONI VITTORIO', 'NIBBI GIULIO', 'ONORI QUIRICO', 'ORTENZI TONINO', 'PANTALONI GIOVANNI', 'PARLATONI DONATELLA', 'PELLONI GIULIA', 'PETROSELLI ANTONIO', 'PISTAGNESI DINO', 'PREMICI CLAUDIO', 'PUGNALONI SIMONE', 'RECCHIONI GABRIELLA', 'ROSA MIRELLA', 'SALVATELLI SAVERIA', 'SAVINI ETTORE', 'SCIALPI MARIA', 'SCOCCIA BRUNA', 'SCOCCIA GELSOMINA', 'SERENA ORESTE', 'SILENZI GIOVANNA', 'SIMONELLI GIAMPIERO', 'SOLLINI SAURO', 'STORTONI CLAUDIO', 'TINTINELLI MARIA GIUSEPPINA', 'TOMASSETTI MARIA TERESA', 'TORRESI MATTIA', 'TRAVAGLINI ALESSANDRA', 'VALENTINI TULLIO', 'VEZZANI GIUSEPPE', 'VINCENZI ANNA', 'AMELIO GIOVANNA', 'BAFFONI GRAZIELLA', 'BARTOCCI EMIDIO', 'BAZZANI ELENA GIORGINA', 'BRANDANI GIOVANNI', 'BRANDIMARTI MORENA', 'CAPITANI GIUSEPPE', 'CARLONI CLAUDIA', 'CARPANI CRISTIANA', 'CENSI GIAMPIERO', 'CICCALENI BIANCA MARIA', 'CICCONI GIANFRANCO', 'CRISTOFORI MARIA', 'CUDINI MARIA PIA', 'CUPELLI GIANFRANCO', 'D''ELENA DANIA', 'DAMEN ALFIO', 'DAMEN LAURA', 'DE MARCELLIS NATALINA', 'DEL ROSSO ANNA MARIA', 'DOLOMITI DOLORES', 'DONATI ADA', 'FARINA GIULIANA', 'FERRI FRANCO', 'FIAMOZZINI IVANA', 'FIORENZA VINCENZO', 'FIORGENTILI ROBERTA', 'FIORI UBALDO', 'FOSSI ANNA', 'FRATALOCCHI DANIELE', 'GENTILI PAOLO', 'GIAMMUSSO FRANCESCO', 'GIAMMUSSO LUCA', 'GIUMENTARO COSIMO', 'IANNONI ELLENI', 'IBETTI LUCIA', 'ILLUMINATI PIETRO', 'LORUSSO VITO', 'MARCONE PAOLO', 'MARCUCCI IVANNA', 'MARONE GIANFRANCO', 'MARONI LUCIANO', 'MARONI MARIO', 'MARZIALI FIORELLA', 'MONTERUBBIANESI ALBERTO', 'MORMILE MICHELINA', 'NARDI RAFAELLA', 'NEPA PRIMO', 'NICCOLINI RANIERA', 'PAGLIALUNGA LUCIANA', 'PALETTI GIOACCHINO', 'PAVONI RAOUL', 'PAZZI ANTONIO', 'PERGOLESI DANIELA', 'PETROCCHI GIORGIO', 'PIERANTOZZI GIOVANNINO', 'PIERDOMINICI LORIANA', 'PIETRACCI LUCIANO', 'POMMELLA RAFFAELE', 'PUOPOLO LUCA', 'RECANATI CLAUDIO', 'RIPANI MARCELLO', 'ROMANELLI IDA', 'RUGGERI ANNA MARIA', 'SANTARELLI VITTORIO', 'SANTINI PIERLUIGI', 'SCOCCIA DANIELE', 'TAVOLETTA GRAZIANO', 'TEODORI ORIETTA', 'TROVATELLI DANIELA', 'ANDRENACCI FLAVIO', 'BAGALINI SONIA', 'CASTELLANI CLAUDIA', 'CHERRI LUIGI', 'CIOBANU IOSIF IRINEL', 'DEL MONTE PIER CIRIACO', 'DI NARDO RODOLFO', 'DI NOCERA ROBERTO', 'DONATI CHIARA', 'DONZELLI VALERIA', 'FELICETTI PIERLUIGI', 'GEROSA GIOVANNI', 'LEONI RENATO', 'LUCCHESI MIRCO', 'LUCIANI LUCIANO', 'LUCIANI PATRIZIA', 'MAGGI AURELIA', 'MAKOWSKA ANETA', 'MARANGONI GIOVANNINA', 'MARINI NELLO', 'MILANTONI MILVA', 'MUCA XHEVDET', 'PETROSELLI CONCETTA', 'PRINCIPI ANNA RITA', 'RIDOLFI ROBERTO', 'SANDRONI ROSA', 'SASSO LAURA', 'SIMONI ANDREA', 'SIMONI ROBERTO', 'SIMONI ROBERTO', 'STACHURZEWSKA WIESLAW DANUTA', 'TEMPERINI VINCENZINA', 'TRAINI BRUNA', 'VALLESI PAOLO') GROUP BY cognome, nome, indirizzo, codicenucleo, tiporifiuto, rifiuto, percent, tot) AS z GROUP BY cognome, nome, indirizzo, codicenucleo ORDER BY indirizzo, cognome, nome;");
            $results = pg_execute($dbconn, "query", array($comune));
            break;
            }
            else{
            $results = pg_prepare($dbconn, "query", "SELECT rd.idcomune, cognome, nome, indirizzo, rd.codicenucleo, Sum(CASE WHEN tiporifiuto = 0 THEN somma END) indifferenziato, Sum(CASE WHEN tiporifiuto = 17 THEN somma END) pannolini, Sum(CASE WHEN tiporifiuto = 16 THEN somma END) barattolame, Sum(CASE WHEN tiporifiuto = 3 THEN somma END) plastica, Sum(CASE WHEN tiporifiuto = 1 THEN somma END) carta, Sum(CASE WHEN tiporifiuto = 15 THEN somma END) vetro, Sum(CASE WHEN tiporifiuto = 4 THEN somma END) organico, Count(*) AS coef, Sum(DISTINCT( tot )) AS tot, telefono, cellulare, mail FROM (SELECT a.idcomune, TRIM(TRAILING FROM e.nome) AS nome, TRIM(TRAILING FROM e.cognome) AS cognome, e.indirizzo, d.codicenucleo, Sum(somma) as somma, a.tiporifiuto, c.nome AS rifiuto, Sum(tot) AS tot FROM (SELECT idcomune, codicetag, sum(peso) AS somma, tiporifiuto FROM conferimenti AS a WHERE dataora::date BETWEEN $2 AND $3 AND id < 6000000 AND tiporifiuto <> 9 AND tiporifiuto <> 7 AND tiporifiuto <> 6 GROUP BY idcomune, codicetag, tiporifiuto) AS a JOIN (SELECT sum(peso) AS tot, codicetag FROM conferimenti WHERE dataora::date BETWEEN $2 AND $3 AND id < 6000000 GROUP BY codicetag) AS b ON a.codicetag=b.codicetag JOIN (SELECT DISTINCT(codicerifiuto), MIN(descrizione) AS nome FROM inforifiuti GROUP BY codicerifiuto) AS c ON a.tiporifiuto=c.codicerifiuto JOIN (SELECT codicetag, codicenucleo FROM associazionenuclei) AS d ON d.codicetag=a.codicetag JOIN (SELECT nome, cognome, indirizzo, codicenucleo FROM datinuclei) AS e ON d.codicenucleo=e.codicenucleo GROUP BY a.idcomune, e.nome, e.cognome, e.indirizzo, d.codicenucleo, a.tiporifiuto, rifiuto ORDER BY d.codicenucleo) AS rd JOIN (SELECT codicenucleo, telefono, cellulare, mail FROM datinuclei WHERE idcomune = $1) AS dati ON rd.codicenucleo = dati.codicenucleo GROUP BY rd.idcomune, cognome, nome, rd.codicenucleo, indirizzo, telefono, cellulare, mail ORDER BY cognome, nome, indirizzo DESC;");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
                }
        }
        case ('codnucleierrati'):
        {
        $results = pg_prepare($dbconn, "query", "SELECT * FROM datinuclei WHERE idcomue = $1 AND codicenucleo NOT SIMILAR TO '[A-Z]{2,3}.[0-9]{4}.[0-9]{4}';");
        $results = pg_execute($dbconn, "query", array($comune));
        break;
        }
        case ('codfiserrati'):
        {
        $results = pg_prepare($dbconn, "query", "SELECT * FROM datinuclei WHERE idcomune = $1 AND codicefiscale NOT SIMILAR TO '[A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z]{3}[A-Z]';");
        $results = pg_execute($dbconn, "query", array($comune));
        break;
        }
        case ('indirizzo'):
        {
            $query = "select * from datinuclei where indirizzo like '%POSTACCHINI%';";
            $results = pg_query($query);
            break;
        }
        case ('report'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT tiponucleo, codicenucleo::text, tag, componenti, max(ultimo) as ultimoconferimento, nominativo,
                                                     indirizzoutenza, codicefiscale, telefono::text, cellulare, mail, CONCAT(indifferenziato::text, ' kg') as indifferenziato, 
                                                     CONCAT(pannolini::text, ' kg') as pannolini, CONCAT(barattolame::text, ' kg') as barattolame,
                                                     CONCAT(plastica::text, ' kg') as plastica, CONCAT(carta::text, ' kg') as carta, CONCAT(vetro::text, ' kg') as vetro,
                                                     CONCAT(vetrobarattolame::text, ' kg') as vetrobarattolame, CONCAT(organico::text, ' kg') as organico, 
                                                     CONCAT(totale::text, ' kg') as totalerifiuti, CONCAT((CASE WHEN (totale-indifferenziato) = 0 or (totale-indifferenziato) = null THEN 0 ELSE 
	                                                 TRUNC(((totale-indifferenziato)/totale) * 100, 2) END)::text, ' %') as percentualedifferenziata, max(stazioni) as stazioni
                                                     from ((SELECT tiponucleo, codicenucleo, ncomponenti as componenti, infonuclei.codicetag as tag, 
                                                     MAX (dataora) as ultimo, CONCAT( nome, ' ', cognome) AS nominativo, indirizzoutenza, codicefiscale, 
													 CASE telefono when '0' THEN '' ELSE telefono END as telefono, CASE cellulare when '0' THEN '' ELSE cellulare END as cellulare,
                                                     CASE mail when '@' THEN '' ELSE mail END as mail, COUNT(peso) as numero_conferimenti, SUM(CASE WHEN tiporifiuto = 0 THEN peso ELSE 0 END) 
													 indifferenziato, SUM(CASE WHEN tiporifiuto = 17 THEN peso ELSE 0 END) pannolini, SUM(CASE WHEN tiporifiuto = 16 THEN peso ELSE 0 END) barattolame,
                                                     SUM(CASE WHEN tiporifiuto = 3 THEN peso ELSE 0 END) plastica, SUM(CASE WHEN tiporifiuto = 1 THEN peso ELSE 0 END) carta,
                                                     SUM(CASE WHEN tiporifiuto = 15 THEN peso ELSE 0 END) vetro, SUM(CASE WHEN tiporifiuto = 2 THEN peso ELSE 0 END) vetrobarattolame,
                                                     SUM(CASE WHEN tiporifiuto = 4 THEN peso ELSE 0 END) organico, SUM(peso) as totale, string_agg(distinct(codicestazione::text), '|') as stazioni
                                                     from datinuclei  left join infonuclei on infonuclei.idnucleo = datinuclei.id left join conferimenti on conferimenti.codicetag = infonuclei.codicetag 
                                                     where  datinuclei.idcomune= $1 AND dataora BETWEEN $2 AND $3::interval + '1 day' group by tiponucleo, codicenucleo, ncomponenti , nome, cognome,
													 indirizzoutenza, codicefiscale, telefono, cellulare, mail, infonuclei.codicetag order by datinuclei.codicenucleo) UNION (SELECT tiponucleo, 
													 codicenucleo, ncomponenti as componenti, infonuclei.codicetag as tag, MAX (dataora) as ultimo, CONCAT( nome, ' ', cognome) AS nominativo, 
													 indirizzoutenza, codicefiscale, CASE telefono when '0' THEN '' ELSE telefono END as telefono, CASE cellulare when '0' THEN '' ELSE 
													 cellulare END as cellulare, CASE mail when '@' THEN '' ELSE mail END as mail, '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0' 
													 from datinuclei  left join infonuclei on infonuclei.idnucleo = datinuclei.id left join conferimenti on 
													 conferimenti.codicetag = infonuclei.codicetag where datinuclei.idcomune= $1 group by tiponucleo, codicenucleo, ncomponenti , nome, cognome, 
													 indirizzoutenza, codicefiscale, telefono, cellulare, mail, infonuclei.codicetag HAVING MAX(dataora) < $2
													 OR MAX(dataora) IS NULL)) as tabella GROUP BY tiponucleo, codicenucleo, tag, nominativo, indirizzoutenza, codicefiscale, 
													 telefono, cellulare, mail, componenti, totale, indifferenziato, pannolini,  barattolame, plastica, carta, vetro, vetrobarattolame, 
													 organico ORDER by totale DESC, percentualedifferenziata ASC;");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
		case ('report2'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT tiponucleo, codicenucleo::text, tag, componenti, max(ultimo) as ultimoconferimento, nominativo,
                                                     indirizzoutenza, codicefiscale, telefono::text, cellulare, mail, CONCAT(indifferenziato::text, ' kg') as indifferenziato, 
                                                     CONCAT(pannolini::text, ' kg') as pannolini, CONCAT(barattolame::text, ' kg') as barattolame,
                                                     CONCAT(plastica::text, ' kg') as plastica, CONCAT(carta::text, ' kg') as carta, CONCAT(vetro::text, ' kg') as vetro,
                                                     CONCAT(vetrobarattolame::text, ' kg') as vetrobarattolame, CONCAT(organico::text, ' kg') as organico, 
                                                     CONCAT(totale::text, ' kg') as totalerifiuti, CONCAT((CASE WHEN (totale-indifferenziato) = 0 or (totale-indifferenziato) = null THEN 0 ELSE 
	                                                 TRUNC(((totale-indifferenziato)/totale) * 100, 2) END)::text, ' %') as percentualedifferenziata, max(stazioni) as stazioni
                                                     FROM (SELECT  tiponucleo, codicenucleo, componenti, string_agg(distinct(tag), ' | ') as TAG, string_agg(distinct(ultimo::text), ' | ') as Ultimo, 
													 string_agg(distinct(stazioni), ' - ') as stazioni, nominativo, indirizzoutenza, codicefiscale, telefono, cellulare, mail, 
													 SUM(numero_conferimenti) numero_conferimenti, SUM(indifferenziato) indifferenziato, SUM(pannolini) pannolini, SUM(barattolame) barattolame, 
                                                     SUM(plastica) plastica, SUM(carta) carta, SUM(vetro) vetro, SUM(vetrobarattolame) vetrobarattolame, SUM(organico) organico, SUM(totale) totale
                                                     FROM ((SELECT tiponucleo, codicenucleo, ncomponenti as componenti, infonuclei.codicetag as tag, 
                                                     MAX (dataora) as ultimo, CONCAT( nome, ' ', cognome) AS nominativo, indirizzoutenza, codicefiscale, 
													 CASE telefono when '0' THEN '' ELSE telefono END as telefono, CASE cellulare when '0' THEN '' ELSE cellulare END as cellulare,
                                                     CASE mail when '@' THEN '' ELSE mail END as mail, COUNT(peso) as numero_conferimenti, SUM(CASE WHEN tiporifiuto = 0 THEN peso ELSE 0 END) 
													 indifferenziato, SUM(CASE WHEN tiporifiuto = 17 THEN peso ELSE 0 END) pannolini, SUM(CASE WHEN tiporifiuto = 16 THEN peso ELSE 0 END) barattolame,
                                                     SUM(CASE WHEN tiporifiuto = 3 THEN peso ELSE 0 END) plastica, SUM(CASE WHEN tiporifiuto = 1 THEN peso ELSE 0 END) carta,
                                                     SUM(CASE WHEN tiporifiuto = 15 THEN peso ELSE 0 END) vetro, SUM(CASE WHEN tiporifiuto = 2 THEN peso ELSE 0 END) vetrobarattolame,
                                                     SUM(CASE WHEN tiporifiuto = 4 THEN peso ELSE 0 END) organico, SUM(peso) as totale, string_agg(distinct(codicestazione::text), '|') as stazioni
                                                     FROM datinuclei  left join infonuclei on infonuclei.idnucleo = datinuclei.id left join conferimenti on conferimenti.codicetag = infonuclei.codicetag 
                                                     where  datinuclei.idcomune= $1 AND dataora BETWEEN $2 AND $3::timestamp + interval '1 day' group by tiponucleo, codicenucleo, ncomponenti , nome, cognome,
													 indirizzoutenza, codicefiscale, telefono, cellulare, mail, infonuclei.codicetag order by datinuclei.codicenucleo) UNION (SELECT tiponucleo, 
													 codicenucleo, ncomponenti as componenti, infonuclei.codicetag as tag, MAX (dataora) as ultimo, CONCAT( nome, ' ', cognome) AS nominativo, 
													 indirizzoutenza, codicefiscale, CASE telefono when '0' THEN '' ELSE telefono END as telefono, CASE cellulare when '0' THEN '' ELSE 
													 cellulare END as cellulare, CASE mail when '@' THEN '' ELSE mail END as mail, '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0' 
													 FROM datinuclei  left join infonuclei on infonuclei.idnucleo = datinuclei.id left join conferimenti on 
													 conferimenti.codicetag = infonuclei.codicetag where datinuclei.idcomune= $1 group by tiponucleo, codicenucleo, ncomponenti , nome, cognome, 
													 indirizzoutenza, codicefiscale, telefono, cellulare, mail, infonuclei.codicetag )) t GROUP BY tiponucleo, codicenucleo, componenti, nominativo, indirizzoutenza, 
                                                     codicefiscale, telefono, cellulare, mail) as tabella GROUP BY tiponucleo, codicenucleo, tag, nominativo, indirizzoutenza, codicefiscale, 
													 telefono, cellulare, mail, componenti, totale, indifferenziato, pannolini,  barattolame, plastica, carta, vetro, vetrobarattolame, 
													 organico ORDER by totale DESC, percentualedifferenziata ASC;");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        case ('soluzione'):
        {
            $results = pg_prepare($dbconn, "query", "SELECT idcomune, codicestazione, codicetag, dataora, idn, data, delta_dataora, riavvio, nuovadataora, flag, conteggio,
                        CASE
                        WHEN riavvio = 2 AND lag(riavvio) OVER (partition by tn.idcomune, tn.codicestazione order by tn.idcomune, tn.codicestazione, tn.idn) = 1 THEN lag(nuovadataora::timestamp) OVER (partition by tn.idcomune, tn.codicestazione, conteggio order by tn.idcomune, tn.codicestazione, tn.idn) + delta_dataora::INTERVAL
                        WHEN riavvio = 2 AND lag(riavvio) OVER (partition by tn.idcomune, tn.codicestazione order by tn.idcomune, tn.codicestazione, tn.idn) = 2 THEN first_value(nuovadataora::timestamp) OVER (partition by tn.idcomune, tn.codicestazione, riavvio, conteggio order by tn.idcomune, tn.codicestazione, tn.idn) + Sum(delta_dataora) FILTER (WHERE delta_dataora > '0 days') OVER (partition by tn.idcomune, tn.codicestazione, conteggio order by tn.idcomune, tn.codicestazione, tn.idn)
                        END AS nuovadataora2
                        FROM(
                        SELECT *, tc.id AS idn, Count(flag) FILTER (WHERE flag=true) OVER (order by tc.idcomune, tc.codicestazione, tc.id) AS conteggio,
                        CASE
                        WHEN riavvio = 1 AND lag(riavvio) OVER (order by tc.idcomune, tc.codicestazione, tc.id) = 0 THEN lag(dataora::timestamp) OVER (order by tc.idcomune, tc.codicestazione, tc.id) + '30 seconds'::INTERVAL
                        WHEN riavvio = 2 AND lag(riavvio) OVER (order by tc.idcomune, tc.codicestazione, tc.id) = 1 THEN lag(dataora::timestamp) OVER (order by tc.idcomune, tc.codicestazione, tc.id) - lag (delta_dataora) OVER (order by tc.idcomune, tc.codicestazione, tc.id)
                        WHEN riavvio = 2 AND lag(riavvio) OVER (order by tc.idcomune, tc.codicestazione, tc.id) = 2 THEN null
                        END AS nuovadataora
                        FROM conferimenti AS tc INNER JOIN 
                        (SELECT id, delta_dataora, true AS flag, 1 AS riavvio FROM (SELECT idcomune AS idc, codicestazione AS cods, id, dataora, lag(dataora) OVER (order by idcomune, codicestazione, id) AS prev_dataora, dataora - lag(dataora) OVER (order by idcomune, codicestazione, id) AS delta_dataora, data, lag(data) OVER (order by idcomune, codicestazione, id) AS prev_data FROM conferimenti WHERE idcomune > 1 AND data > '1900-01-01') as t1 WHERE delta_dataora < '0 days'
                        UNION
                        SELECT id, delta_dataora, false AS flag, 0 AS riavvio FROM (SELECT idcomune AS idc, codicestazione AS cods, id, dataora, lag(dataora) OVER (order by idcomune, codicestazione, id) AS prev_dataora, dataora - lag(dataora) OVER (order by idcomune, codicestazione, id) AS delta_dataora, data, lag(data) OVER (order by idcomune, codicestazione, id) AS prev_data FROM conferimenti WHERE idcomune > 1 AND data > '1900-01-01' AND dataora > '2016-12-31') as t2 WHERE delta_dataora > '0 days'
                        UNION
                        SELECT id, delta_dataora, false AS flag, 2 AS riavvio FROM (SELECT idcomune AS idc, codicestazione AS cods, id, dataora, lag(dataora) OVER (order by idcomune, codicestazione, id) AS prev_dataora, dataora - lag(dataora) OVER (order by idcomune, codicestazione, id) AS delta_dataora, data, lag(data) OVER (order by idcomune, codicestazione, id) AS prev_data FROM conferimenti WHERE idcomune > 1 AND data > '1900-01-01' AND dataora < '2016-12-31') as t3 WHERE delta_dataora > '0 days' ORDER BY id)
                        AS tj ON tc.id = tj.id
                        WHERE tc.id > 5990000
                        ORDER BY tc.idcomune, tc.codicestazione, tc.id
                        ) AS tn
                        WHERE idcomune = $1 AND codicestazione = $2
                        ORDER BY tn.idcomune, tn.codicestazione, tn.idn
                        LIMIT 6000;");
            $results = pg_execute($dbconn, "query", array($comune, $stazione));
            break;
        }
        case ('medieprocapite'):
        {
            $results = pg_prepare($dbconn, "query", "select a.idcomune, tiporifiuto, descrizione, count(a.codicenucleo) as nuclei, sum(d.ncomponenti) as componenti, sum(conf) as conferimenti, sum(peso) chili, max(c.dataora)
                    from associazionenuclei a inner join datinuclei d on d.codicenucleo=a.codicenucleo
                    inner join 
                     (select codicenucleo, max(dataora) as dataora, count(conferimenti.id) as conf, tiporifiuto, sum(peso) as peso
                      from conferimenti inner join infonuclei on conferimenti.codicetag = infonuclei.codicetag where dataora BETWEEN $2 AND $3 AND infonuclei.idcomune = $1 group by codicenucleo, tiporifiuto) c on c.codicenucleo=a.codicenucleo
                    inner join inforifiuti i on a.idcomune=i.idcomune AND c.tiporifiuto=i.codicerifiuto
                    where c.dataora BETWEEN $2 AND $3 AND a.idcomune = $1
                    group by a.idcomune, tiporifiuto, descrizione
                    order by idcomune, tiporifiuto;");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
			case ('inviodati'):
        {
            $results = pg_prepare($dbconn, "query", "Select distinct comune.nome, infostazioni.codicestazione, last.ultimodato as ultimoinvio from (Select id, nome from infocomune) as comune 
                                                     inner join infostazioni on infostazioni.idcomune=comune.id inner join (Select distinct c.idcomune, c.codicestazione, (CASE WHEN max1 > max2 THEN 
                                                     (CASE WHEN max1 > max3 THEN max1 ELSE max3 END) ELSE (CASE WHEN max2 > max3 THEN max2 ELSE max3 END) END) as ultimodato from (Select distinct idcomune, codicestazione, 
                                                     max(dataora) as max1 from conferimenti group by idcomune, codicestazione) as c right join (Select distinct idcomune, idstazione, 
                                                     max(dataora) as max2 from eventi  group by idcomune, idstazione) as e on e.idstazione=c.codicestazione AND e.idcomune=c.idcomune
                                                     right join (Select distinct idcomune, codicestazione, max(dataora) as max3 from logoperatore  group by idcomune, codicestazione) 
                                                     as l on l.codicestazione=e.idstazione AND l.idcomune=e.idcomune group by c.idcomune, c.codicestazione, max1 , max2, max3) as last
                                                     on last.idcomune=infostazioni.idcomune AND last.codicestazione=infostazioni.codicestazione where (last.ultimodato::date < ($1::timestamp)::date
                                                     or last.ultimodato::date > ($1::timestamp)::date) AND last.ultimodato not BETWEEN ($1::timestamp - interval '12h')::timestamp AND $1::timestamp AND
                                                     infostazioni.idcomune IS NOT NULL AND infostazioni.codicestazione IS NOT NULL order by comune.nome, infostazioni.codicestazione;");
            $results = pg_execute($dbconn, "query", array($fine));
            break;
        }
        case ('medieprocapitetotali'):
        {
            $results = pg_prepare($dbconn, "query", "select a.idcomune, count(a.codicenucleo) as nuclei, sum(d.ncomponenti) as componenti, 
			                                        sum(conf) as conferimenti, max(c.dataora) from associazionenuclei a inner join datinuclei d on d.codicenucleo=a.codicenucleo
                                                    inner join (select codicenucleo, max(dataora) as dataora, count(conferimenti.id) as conf from conferimenti inner join infonuclei on 
				                                    conferimenti.codicetag = infonuclei.codicetag group by codicenucleo) c on c.codicenucleo=a.codicenucleo where c.dataora BETWEEN $2 AND $3 AND a.idcomune = $1
                                                    group by a.idcomune order by idcomune;");
            $results = pg_execute($dbconn, "query", array($comune, $inizio, $fine));
            break;
        }
        default:
		{
		$results = pg_prepare($dbconn, "query", "SELECT codice , Coordinate , CONCAT( nome , ' ' ,Stazione) AS Postazione , Problema , descrizione , st.stato , manutentore , CommentoEsecuzione , 
		                                         CommentoRisoluzione , dataIntervento , utente from (SELECT em.id AS codice, infocomune.nome  AS nome, infostazioni.codicestazione AS Stazione,
                                                 CONCAT( infostazioni.latitudine , ' , ' , infostazioni.longitudine ) AS Coordinate, manutentore , dataoraproblema AS Data , 
												 CONCAT(sigla, ' ' , ii.descrizione) AS Problema ,  em.descrizione , stato , commento1 AS CommentoEsecuzione , commento2 AS CommentoRisoluzione, 
												 dataorarisoluzione As dataIntervento , utente FROM infocomune left join infostazioni on infostazioni.idcomune = infocomune.id left join 
												 eventimanutenzione em on em.idcomune = infostazioni.idcomune AND em.codicestazione = infostazioni.codicestazione left join infointerventi ii 
												 on ii.id = em.codiceproblema where em.idcomune = $1 group by em.id , infocomune.nome , infostazioni.codicestazione ,infostazioni.latitudine , 
												 infostazioni.longitudine , sigla , ii.descrizione order by dataoraproblema) em left join stato st on st.id = em.stato 
												 ".(($_SESSION["usertype"]==4)?"WHERE st.stato NOT LIKE '%chiuso%' ":" ").";");
        $results = pg_execute($dbconn, "query", array($comune));
        break;
		}
    }
	if(pg_num_rows($results)>0)
    {
	echo "<table id='risultati' class='table table-bordered table-striped table-hover' cellspacing='0' width='100%'>\n";
    //header tabella
    echo "<thead>\n<tr>";
     $i = pg_num_fields($results);
        for ($j = 0; $j < $i; $j++) {
        $fieldname = pg_field_name($results, $j);
		echo '<th>'. ucfirst($fieldname) .'</th>';
        }
    echo "</tr>\n</thead>";
    //footer tabella
    echo "<tfoot>\n<tr>";
     $i = pg_num_fields($results);
        for ($j = 0; $j < $i; $j++) {
        $fieldname = pg_field_name($results, $j);
		echo '<th>' . ucfirst($fieldname) . '</th>';
		}
    echo "</tr>\n</tfoot>";
    //fine tabella "universale"   
	$file['risultati']= pg_fetch_all($results,PGSQL_NUM);
	$data=json_encode($file);
	file_put_contents('../phpinc/'.session_id().'.json', $data);
	unset($data);
        //modifiche da finire per il grafico
        if ($mode == "mediecumuli"){
        $arr = pg_fetch_all($results);
		foreach ($arr as $n => $item)
		$arraymedie[]=$item['mediapeso'];
		print_r($arraymedie);
		$ax=$arr[0]['nomerifiuto'];
		$ay=$arr[0]['mediapeso'];
        //free memory associated with result
        //$results->close();
        //now print the data
		echo '<pre>';
		print_r($ax);
		print_r($ay);
		print_r($arr);
		//print json_encode($arr);
		$jdata = json_encode($arr);
		print $jdata;
		echo '</pre>';
        }
    echo "</table>\n";
	}
    pg_close($dbconn);
?>