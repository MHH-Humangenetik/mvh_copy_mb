Dokumentation Meldebestätigung MVGenomSeq

# Dokumentation Aufbau und Beschreibung Meldebestätigung im Modellvorhaben
## Genomsequenzierung nach § 64e SGB V

Dokumentname: Dokumentation Meldebestätigung MVGenomSeq, Version Nr.: 01

# Rechtliche Grundlagen

Gemäß § 6 der Genomdatenverordnung (GenDV) sind die klinischen Datenknoten (KDK) sowie die Genomrechenzentren (GRZ) des Modellvorhabens Genomsequenzierung zur Qualitätskontrolle eingehender Datensätze verpflichtet. Das Bundesinstitut für Arzneimittel und Medizinprodukte (BfArM) gibt in seiner Rolle als Plattformträger die Prüfkriterien vor. Nach der Prüfung der Daten auf Plausibilität und Vollständigkeit übermitteln KDK und GRZ das Ergebnis der Prüfung in Form eines Prüfberichts an den Plattformträger.

Abhängig vom Inhalt des Prüfberichts übersendet der Plattformträger eine Meldebestätigung an den Leistungserbringer. Dabei wird vom Plattformträger im Regelfall für einen klinischen Fall sowohl eine Meldebestätigung basierend auf dem Prüfbericht zu den genomischen Daten als auch eine Meldebestätigung basierend auf dem Prüfbericht zu den klinischen Daten generiert.

Da im Rahmen des Modellvorhabens Genomsequenzierung ausschließlich eine Pauschale je Teilnahme einer versicherten Person bei den Kostenträgern abgerechnet werden kann, wird nur für die Erstmeldung eines genomischen bzw. klinischen Datensatzes nach bestandener Qualitätskontrolle eine Meldebestätigung vom Plattformträger an den Leistungserbringer übermittelt.

In der Startphase des Modellvorhabens wird die Meldebestätigung gemeinsam mit der Vorgangsnummer in einer CSV-Datei als Anhang einer S/MIME-verschlüsselten E-Mail übermittelt. Um die Meldebestätigung analog zu Vorgaben des Bundesamts für Sicherheit in der Informationstechnik (BSI) und des Bundesbeauftragten für den Datenschutz und die Informationsfreiheit (BfDI) verschlüsselt übermitteln zu können, ist es notwendig, dass die Leistungserbringer dem Plattformträger den öffentlichen Schlüssel (public key) ihres S/MIME Zertifikats zukommen lassen.

Um sicherzustellen, dass bei der Anwendung des Hash-Algorithmus auf den Hash-String immer der gleiche Hashwert erzeugt wird, müssen entsprechende technische Vorgaben definiert werden. Zudem müssen einheitliche Zeichensätze bzw. Encodings verwendet werden oder es muss ein Zeichenvorrat definiert werden, der in allen zu Anwendung kommenden Zeichensätzen einheitlich codiert wird.

# Zeichensatz und erlaubte Zeichen:

Die Vorgaben für den elektronischen Datenaustausch zwischen Krankenhäusern und Krankenkassen nach § 301 Abs. 3 SGB V legen folgende Zeichensätze für die Datenübermittlung fest:

- Code gemäß DIN 66303:2000-06 Deutsche Referenzversion des 8-Bit-Code (der DRV 8-Bit-Code nach DIN 66303:2000-06 entspricht in Umfang und Zeichenanordnung dem international standardisierten ISO 8859-1)
- Code gemäß DIN 66003 DRV Deutsche Referenzversion des 7-Bit-Code
- ISO 8859-15
- ISO 8859-1

Ein einheitlicher Zeichensatz kann somit für die Beteiligten nicht vorausgesetzt werden. Es muss daher ein Zeichenvorrat für die Angaben in der Meldebestätigung und für den alphanumerischen Code der

Meldebestätigung definiert werden, der in allen von den Beteiligten verwendeten Zeichensätzen einheitlich codiert wird. Dabei müssen jedoch die EDIFACT-Steuerzeichen [+ : ' ?] ausgenommen werden. Die Datenaustauschverfahren zwischen den Krankenhäusern und den Krankenkassen nutzen EDIFACT als Datenformat. Die Steuerzeichen dürfen nicht im Inhalt der Meldebestätigung verwendet werden, da sonst Syntaxfehler im Datenaustausch die Folge wären. Darüber hinaus muss für die Notation der Zeichenkette, bestehend aus dem alphanumerischen Code der Meldebestätigung und den weiteren in der Meldebestätigung enthaltenen Informationen (Hash-String), ein Trennzeichen („&amp;“) zwischen den Elementen definiert werden.

Auf dieser Grundlage dürfen ausschließlich folgende Zeichen für die alphanumerische Meldenachweis-Nummer und für die weiteren in der Meldebestätigung enthaltenen Informationen zugelassen sein:

[a-z][A-Z][0-9][&amp;]

Unter Einhaltung der vorgenannten Bedingungen könnten weitere Zeichen ergänzt werden, sofern diese vom Plattformträger für die Bildung des zehnstelligen Codes für die Meldebestätigung benötigt werden.

Die den Hash-String der Meldebestätigung bildenden Informationen sind:

**Alphanumerischer Code der Meldebestätigung:**
Der alphanumerische Code der Meldebestätigung ist exakt **zehnstellig** und wird vom Plattformträger kollisionsfrei für jeden eingehenden Prüfbericht zufällig generiert.

**Leistungsdatum zzgl. eines Zählers:**
Beim Datum der Leistungserbringung (Leistungsdatum) handelt es sich nach §11 Abs. 4 des 64e-Vertrags zwischen GKV-SV und den Leistungserbringern um das Datum des Abschlusses der vollständigen und richtigen Übermittlung des Datensatzes an den Datenknoten, konkret um das Datum der Übermittlung des Prüfberichts zu einem Datensatz mit erfolgreicher Qualitätskontrolle von Genomrechenzentrum (GRZ) bzw. klinischem Datenknoten (KDK) an den Plattformträger BfArM. Das Datum hat das Format JJJJMMTT. Da ein Leistungserbringer mehrere Datensätze an einem Tag versenden kann, wird zusätzlich ein dreistelliger Zähler (ZZZ) dem Datum angehängt. Der Zähler bezieht sich auf die Datensätze pro Leistungserbringer pro Datum, beginnend mit 001. Das Gesamtformat dieses Eintrags ist also JJJJMMTTZZZ.

**ID des Leistungserbringers:**
Bei der ID des Leistungserbringers handelt es sich um einen eineindeutigen Identifikator, der eine Zuordnung der Meldebestätigung zu einem Leistungserbringer ermöglicht. Diese ID entspricht dem Institutionskennzeichen des Leistungserbringers gemäß § 293 SGB V und wird vom Plattformträger mit jedem Leistungserbringer abgeglichen.

**ID des Datenknoten:**
Für die Zuordnung der klinischen bzw. genomischen Daten und der Prüfberichte und Meldebestätigungen zu einem klinischen Datenknoten bzw. Genomrechenzentrum wird bei Zulassung der Datenknoten vom Plattformträger jedem Knoten eine eineindeutige ID zugewiesen. Diese hat für klinische Datenknoten den Aufbau KDKXXXnnn und für Genomrechenzentren den Aufbau GRZXXXnnn.

Dokumentation Meldebestätigung MVGenomSeq

Dokumentation Meldebestätigung MVGenomSeq

# Typ der Meldung:

Da für einen Fall unterschiedliche Meldungstypen der Daten auftreten können, muss eine Unterscheidung zwischen Erstmeldungen, Follow-Ups, Nachmeldungen und Korrekturen möglich sein. Zu Abrechnungszwecken werden nur Meldebestätigungen zu Initialmeldungen mit bestandener Qualitätskontrolle an die Abrechnungsstelle der Leistungserbringer übermittelt. Eine Erstmeldung (Typ der Meldung: 0) mit bestandener Qualitätskontrolle (Ergebnis der Qualitätskontrolle: 1) beschreibt den ersten übermittelten Prüfbericht durch den KDK bzw. das GRZ an den Plattformträger.

# Indikationsbereich:

Für die Differenzierung zwischen onkologischen und Seltenen Erkrankungen wird durch ein O bzw. R (für „rare“) der Indikationsbereich in der Meldebestätigung beschrieben, außerdem werden die hereditären Tumorprädispositionssyndrome mit einem H beschrieben.

# Produktzuordnung:

Die Produktzuordnung dient der eindeutigen Zuordnung der Meldebestätigung innerhalb des IBE-Segments zum Modellvorhaben MV GenomSeq nach § 64e SGB V und wird in diesem Zusammenhang immer mit „9“ ausgefüllt.

# Kostenträger:

Zur Zuordnung einer Meldebestätigung zum entsprechenden Krankenversicherungssystem (Gesetzlich/GKV, Privat/PKV, Privat mit Beihilfe, andere) und zu Vereinfachung der Qualitätssicherung wird die dem Patienten zugehörige Versicherungsform angegeben. Die Angabe „andere“ beschreibt alle Sonderfälle (bspw. Selbstzahler).

# Art der Daten:

Da es jeweils eine Meldebestätigung für klinische Daten und genomische Daten geben wird, muss zwischen diesen beiden unterschieden werden können.

# Art der Sequenzierung:

Um für die Abrechnung genauer zwischen den Sequenzierungsarten (WGS/WES/Panel/WGS_LR) unterscheiden zu können, wird diese Information numerisch kodiert in der Meldebestätigung enthalten sein. Für den Fall, dass sich in der Fallkonferenz gegen die Durchführung einer Genomsequenzierung entschieden wird (z.B., weil ein Fall ohne Sequenzierung gelöst werden konnte), ist ebenfalls eine Kodierung vorgesehen (Art der Sequenzierung: „keine [0]“).

# Ergebnis der Qualitätskontrolle:

Die Qualität der Daten wird seitens der GRZ und KDK nach Vorgaben des Plattformträgers kontrolliert und das Ergebnis der Prüfung binär in der Meldebestätigung angegeben. Nur eine Meldebestätigung mit erfolgreicher QC („bestanden [1]“) kann für eine Abrechnung benutzt werden.

Inhalte und Formate des Hash-Strings einer Meldebestätigung:

|  Inhalt | Status * | Format * | Bemerkung  |
| --- | --- | --- | --- |
|  Alphanumerischer Code der Meldebestätigung | M | an10 | A123456789  |
|  Leistungsdatum zzgl. eines 3-stelligen Zählers | M | n11 | JJJJMMTTZZZ, z.B. 20240701001  |
|  ID des Leistungserbringers | M | an9 | Haupt-Institutionskennzeichen gemäß § 293 SGB V  |
|  ID des Datenknoten | M | an9 | z.B. KDKK00001  |
|  Typ der Meldung | M | n1 | Erstmeldung [0], Follow-Up [1] Nachmeldung [2], Korrektur [3]  |
|  Indikationsbereich | M | a1 | Onkologische Erkrankung [O], Seltene Erkrankung [R], Hereditäres Tumorprädispositionssyndrom [H]  |
|  Produktzuordnung | M | n1 | Keine [0], spezialangefertigtes Implantat oder Implantat mit Sonderzulassung [1], Zuordnung MV GenomSeq [9]  |
|  Kostenträger | M | n1 | GKV [1], PKV [2], PKV/Beihilfe [3], andere [4]  |
|  Art der Daten | M | a1 | Klinische Daten [C], genomische Daten [G]  |
|  Art der Sequenzierung | M | n1 | Keine [0], WGS [1], WES [2], Panel [3], WGS_LR [4]  |
|  Ergebnis der Qualitätskontrolle # | M | n1 | Nicht bestanden [0], bestanden [1]  |

Als Trennzeichen zwischen den angegebenen Informationen innerhalb des Hash-Strings der Meldebestätigung ist das „&amp;“-Zeichen anzugeben.

* Erläuterungen: Die Spalte „Status“ unterteilt sich in „Muss (M)“ - und „Kann (K)“ - Parameter. „Format“ beschreibt den Aufbau und die Länge der Information, d.h. „alphanumerisch (an)“ + Länge, „numerisch (n)“ + Länge oder „alphabetisch (a)“ + Länge.

# Nur ein Datensatz mit bestandener Qualitätskontrolle (QC = „bestanden [1]“) kann von den Leistungserbringern zur Abrechnung benutzt werden.

Dokumentation Meldebestätigung MVGenomSeq

Dokumentation Meldebestätigung MVGenomSeq

# Syntax des Hash-Strings für eine Meldung:

[Alphanumeric Code der Meldebestätigung]&amp;[Datum der Leistungserbringung zzgl. eines Zählers]&amp;[ID des Leistungserbringers]&amp;[ID des Netzwerks]&amp;[Typ der Meldung]&amp;[Indikationsbereich]&amp;[Produktzuordnung]&amp;[Kostenträger]&amp;[Art der Daten]&amp;[Art der Sequenzierung]&amp;[Ergebnis der Qualitätskontrolle]

(Länge: 56 Zeichen)

# Hash-Algorithmus:

Hier wird SHA256 verwendet. Die Ausgabe der Hashfunktion wird als Hexadezimalzahl (Länge: 64 Zeichen) angegeben.

Beispiel:

|  Alphanumeric Code der Meldebestätigung | A123456789  |
| --- | --- |
|  Datum der Leistungserbringung
zzgl. eines Zählers | 20240701001  |
|  ID des Leistungserbringers | 260530103  |
|  ID des Datenknoten | KDKK00001  |
|  Typ der Meldung | 0  |
|  Indikationsbereich | O  |
|  Produktzuordnung | 9  |
|  Kostenträger | 1  |
|  Art der Daten | C  |
|  Art der Sequenzierung | 2  |
|  Ergebnis der Qualitätskontrolle | 1  |
|  Hash-String | A123456789&20240701001&260530103&KDKK00001&0&0&9&1&C&2&1  |
|  Hash-Wert (SHA256) | bad8a31b1759b565bee3d283e68af38e173
499bfccce2f50691e7eddda62b2f31  |

Dieses Beispiel zeigt die Meldebestätigung zu einer Erstmeldung klinischer Daten eines GKV-Patienten. Es handelt sich um einen onkologischen Fall mit Whole Exome - Sequenzierung und bestandener QC. Die Daten sind nach Übermittlung vom Leistungserbringer mit ID 260530103 an den KDK mit der ID KDKK00001 vom Datenknoten qualitätsgeprüft worden. Am 07.01.2024 sind die Daten vom KDK gegenüber dem Plattformträger in Form eines Prüfberichts als vollständig und korrekt in Inhalt und Umfang bestätigt worden. Dabei handelt es sich um den ersten Datensatz des Tages (ZZZ=001) dieses Leistungserbringers.

5

Dokumentation Meldebestätigung MVGenomSeq

# Vorgaben zur Übermittlung in Datenübermittlungsverfahren zu Abrechnungszwecken

Die Übermittlung der Meldebestätigung zählt zu den abrechnungsbegründenden Unterlagen. Ein Fall kann hierbei mehrere Meldebestätigungen umfassen.

In der Datenübermittlung sind je Meldebestätigung folgende Angaben in einer wiederholbaren Segmentgruppe zu übermitteln:

|  Segment | Inhalt | Status | Format | Bemerkung  |
| --- | --- | --- | --- | --- |
|  64e* | Segment Datenübermittlung Modellvorhaben | K | an3 | „64e“ (10x möglich)  |
|  IBE | Segment implantatbezogene Eingriffe | K | an3 | „IBE“ (10x möglich)  |
|   |  ID Meldebestätigung | M | an10 |   |
|   |  Hash-String | M | an..512 |   |
|   |  Produktzuordnung | M | n1 | „9“ (MV GenomSeq)  |
|   |  Hashwert | M | an64 |   |

(*Zukünftig könnte ein Modellvorhaben-spezifisches Segment, z.B. „64e“, benutzt werden.)

## Beispiel:

IBE+A123456789+A123456789&amp;20240701001&amp;260530103&amp;KDKK00001&amp;0&amp;0&amp;9&amp;1&amp;C&amp;2&amp;1+9+ bad8a31b1759b565bee3d283e68af38e173499bfcce2f50691e7eddda62b2f31

## Anmerkungen:

Abrechnungsrelevant sind ausschließlich Meldebestätigungen mit dem Typ „Erstmeldung“ („0“ an entsprechender Stelle des Hash-Strings) und bestandener Qualitätskontrolle („1“ an der letzten Stelle des Hash-String).

Für die vollständige Abrechnung der Leistung müssen die Leistungserbringer in der Regel jeweils zwei Meldebestätigungen (je eine für Datentyp C und G) mit QC=1 („bestanden“) pro Fall einreichen.

**Sonderfall:** Wenn in der initialen Fallkonferenz der klinische Fall bereits gelöst werden kann und keine Genomsequenzierung erfolgen soll, wird nur die Meldebestätigung zu den klinischen Daten (Datentyp = C) mit Art der Sequenzierung = 0 („keine“) vorgelegt, um die Fallpauschale abrechnen zu können.

Datentyp=C + Art der Sequenzierung=0 + QC=1 → Fallpauschale ohne Genomsequenzierung

Der Gesamtstring der Meldebestätigung wird dem Leistungserbringer als Textstring und auch als QR Code (Bildformat: PNG) bereitgestellt.

Dokumentation Meldebestätigung MVGenomSeq