<p align="center"><img src="docs/site/src/assets/logo.svg" alt="Das Ptah-Logo: ein bernsteinfarbener Deckstein über zwei hellblauen Steinlagen auf einem dunklen Quadrat mit abgerundeten Ecken" width="72" height="72"></p>

<h1 align="center">Ptah</h1>

<p align="center">Open-Source-Verwaltung von Datenbankänderungen für Schemas und persistenten Inferenzzustand.</p>

<p align="center"><a href="README.md">English</a> · <a href="README.ja.md">日本語</a> · <strong>Deutsch</strong> · <a href="README.fr.md">Français</a></p>

<p align="center">
  <a href="https://github.com/stokaro/ptah/actions/workflows/go-unit-tests.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/go-unit-tests.yml?branch=master&label=tests&logo=github" alt="Status der Unit-Tests auf master"></a>
  <a href="https://github.com/stokaro/ptah/actions/workflows/go-integration-tests.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/go-integration-tests.yml?branch=master&label=integration&logo=github" alt="Status der Integrationstests auf master"></a>
  <a href="https://github.com/stokaro/ptah/actions/workflows/capability-matrix.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/capability-matrix.yml?branch=master&label=databases&logo=github" alt="Status der Funktionsmatrix auf master, die jede deklarierte Datenbank-Versionsreihe prüft"></a>
  <a href="https://github.com/stokaro/ptah/releases/latest"><img src="https://img.shields.io/github/v/release/stokaro/ptah?label=release&logo=github" alt="Das neueste veröffentlichte Release-Tag"></a>
  <a href="https://pkg.go.dev/ptah.run"><img src="https://pkg.go.dev/badge/ptah.run.svg" alt="Go-Paketreferenz für ptah.run"></a>
  <a href="https://github.com/stokaro/ptah/blob/master/go.mod"><img src="https://img.shields.io/github/go-mod/go-version/stokaro/ptah?label=go%20%E2%89%A5&logo=go&logoColor=white" alt="Die in go.mod angegebene niedrigste Go-Version, mit der sich dieses Modul kompilieren lässt"></a>
  <a href="https://github.com/stokaro/ptah/blob/master/LICENSE"><img src="https://img.shields.io/github/license/stokaro/ptah?label=license&color=blue" alt="Lizenz: MIT"></a>
</p>

<p align="center"><a href="#installation">Installation</a> · <a href="https://docs.ptah.run/edge/start/quick-start/">Schnellstart</a> · <a href="https://docs.ptah.run/edge/inference/overview/">Inferenzmigrationen</a> · <a href="https://docs.ptah.run/edge/">Dokumentation</a> · <a href="https://docs.ptah.run/edge/databases/support-matrix/">Datenbankunterstützung</a></p>

<p align="center">
  <a href="https://docs.ptah.run/edge/databases/postgresql/" title="PostgreSQL"><img src="docs/assets/engines/postgresql.svg" alt="PostgreSQL" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/mysql/" title="MySQL"><img src="docs/assets/engines/mysql.svg" alt="MySQL" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/mysql/" title="MariaDB"><img src="docs/assets/engines/mariadb.svg" alt="MariaDB" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/sqlite/" title="SQLite"><img src="docs/assets/engines/sqlite.svg" alt="SQLite" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/sqlserver/" title="SQL Server"><img src="docs/assets/engines/sqlserver.svg" alt="SQL Server" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/clickhouse/" title="ClickHouse"><img src="docs/assets/engines/clickhouse.svg" alt="ClickHouse" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/distributed/" title="CockroachDB"><img src="docs/assets/engines/cockroachdb.svg" alt="CockroachDB" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/distributed/" title="YugabyteDB"><img src="docs/assets/engines/yugabytedb.svg" alt="YugabyteDB" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/oracle/" title="Oracle"><img src="docs/assets/engines/oracle.svg" alt="Oracle" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/distributed/" title="Spanner"><img src="docs/assets/engines/spanner.svg" alt="Spanner" height="32" width="32"></a>
</p>

Ptah verwaltet Änderungen an Datenbankschemas und persistentem Inferenzzustand.
Für Schemas vergleicht es das gewünschte Schema mit einer laufenden Datenbank.
Daraus erzeugt es versionierte Migrationen oder wendet einen freigegebenen Plan
direkt an. Für Inferenzzustand baut es eine neue Generation neben der aktiven
auf, ruft einen externen Embedding-Endpunkt auf, prüft das Ergebnis und stellt
die Verbraucher mit einer Rückkehrmöglichkeit um.

Die Kommandozeile benötigt keine Go-Toolchain. Dieselben Planungskomponenten
sind auch als Go-Pakete verfügbar.

## Schemaänderungen

<p align="center"><img src="docs/site/src/assets/product-journeys.svg" alt="Schemaquellen und eine laufende Datenbank ergeben einen prüfbaren Plan, der entweder zu versionierten Migrationsdateien wird oder direkt angewendet wird. Inferenzspezifikationen und Quelldaten ergeben eine Kandidatengeneration, die vor der Umschaltung geprüft wird. Die aktive Generation bleibt für einen Rollback verfügbar." width="1000"></p>

Beide Abläufe verwenden dasselbe Vergleichs- und Planungsmodell. Der Unterschied
liegt darin, ob das SQL vor der Ausführung als geprüftes Artefakt in der
Versionsverwaltung landet.

## Persistenter Inferenzzustand

Ptah orchestriert die Migration; es führt keine Inferenz aus. Es liest
Quelldaten, ruft den externen Endpunkt auf und schreibt die Kandidatengeneration
selbst. Die aktive Generation bleibt bis zur Prüfung und Umschaltung unverändert.

<p align="center"><img src="docs/site/src/assets/inference-generation-lifecycle.svg" alt="Die aktive Inferenzgeneration beantwortet weiterhin Abfragen, während Ptah einen Kandidaten vorbereitet, befüllt, aktualisiert, indiziert und prüft. Die Umschaltung aktiviert den geprüften Kandidaten. Ein Rollback kann die aufbewahrte Vorgängergeneration wiederherstellen; ihre Entfernung erfolgt separat und ist destruktiv." width="1000"></p>

Die [Anleitung zu Inferenzmigrationen](https://docs.ptah.run/edge/inference/overview/)
behandelt Spezifikation, Übernahme gleichzeitiger Änderungen,
Auswertung, Freigaben, Rollback und Entfernung alter Generationen.

> [!NOTE]
> Ptah befindet sich vor der allgemeinen Verfügbarkeit (pre-GA). Die native
> Befehlsstruktur und die öffentliche Go-API können sich noch ändern.

## Installation

Das Installationsskript wählt das aktuelle Release für Linux, macOS oder
Windows, prüft dessen Prüfsumme und installiert `ptah`, `ptah-compat` und
`ptah-ls` in Ihrem Benutzerverzeichnis.

```bash
curl -fsSL https://ptah.run/install.sh | sh
```

In PowerShell:

```powershell
irm https://ptah.run/install.ps1 | iex
```

Die [Installationsanleitung](https://docs.ptah.run/edge/start/install/)
erklärt feste Versionsauswahl, Signaturprüfung, Download ohne
Ausführung und den Build aus dem Quellcode.

## Ptah mit SQLite ausprobieren

Speichern Sie dieses gewünschte Schema als `schema.sql`:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE
);
```

Geben Sie das SQL aus, wenden Sie es auf eine temporäre Datenbank an und prüfen
Sie, ob die Datenbank mit der Datei übereinstimmt:

```bash
ptah schema render --schema-file schema.sql --dialect sqlite
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --auto-approve
ptah schema drift --db-url "sqlite://app.db" --schema-file schema.sql
```

Die erwartete Ausgabe enthält:

```text
CREATE TABLE "users" (
```

Die erwartete Ausgabe enthält:

```text
Schema apply completed successfully.
```

Die erwartete Ausgabe enthält:

```text
No schema drift detected.
```

Der letzte Befehl endet mit Code 0, wenn die Datenbank der Datei entspricht.
Damit eignet er sich als CI-Prüfung. Entfernen Sie anschließend `app.db` und
`schema.sql`.

> [!CAUTION]
> `--auto-approve` überspringt die Bestätigung. Eine direkte Schemaänderung
> kann Objekte entfernen, die das gewünschte Schema nicht deklariert. Verwenden
> Sie es hier nur, weil `app.db` eine entbehrliche Testdatenbank ist.

Einen vollständigen Ablauf mit erwarteter Ausgabe und Prüfung bieten die
[Einführung in direkte Schemaänderungen](https://docs.ptah.run/edge/start/quick-start-direct/)
und die
[Einführung in versionierte Migrationen](https://docs.ptah.run/edge/start/quick-start-migrations/).

## Den Ablauf für Schemaänderungen wählen

| Ablauf | Geeignet, wenn | Einstieg |
| --- | --- | --- |
| Versionierte Migrationen | SQL-Dateien in Code-Review und Deployment-Verlauf gehören | `ptah migrations generate` |
| Direkte Schemaänderungen | Das gewünschte Schema maßgeblich ist und der Plan sofort geprüft und angewendet werden soll | `ptah schema plan` |

Schemaquellen können SQL, YAML, HCL, DBML, Go-Annotationen, externe Ladeprogramme
oder eine laufende Datenbank sein. Der Funktionsumfang hängt vom Datenbankmotor
ab. Prüfen Sie die [Supportmatrix](https://docs.ptah.run/edge/databases/support-matrix/)
und `ptah db capabilities --db-url <url>` für das konkrete Ziel.

## Dokumentation erkunden

Die folgenden Anleitungen sind auf Englisch:

- [Einen Ablauf wählen](https://docs.ptah.run/edge/start/choose-a-workflow/):
  versionierte Migrationen und direkte Schemaänderungen vergleichen.
- [Eine laufende Datenbank untersuchen](https://docs.ptah.run/edge/direct/inspect/)
  oder [Schemas vergleichen und Drift erkennen](https://docs.ptah.run/edge/direct/compare-and-drift/).
- [Migrationsintegrität prüfen](https://docs.ptah.run/edge/versioned/integrity-and-safety/)
  oder [Migrationen und Schemas testen](https://docs.ptah.run/edge/testing/migrations-and-schema/).
- Ein Schema [visualisieren](https://docs.ptah.run/edge/schema/visualize/)
  oder [exportieren](https://docs.ptah.run/edge/schema/export/).
- [Persistenten Inferenzzustand migrieren](https://docs.ptah.run/edge/inference/overview/),
  während ein externer Endpunkt die Embeddings berechnet.
- [Native Befehle nachschlagen](https://docs.ptah.run/edge/reference/native-commands/)
  oder [einen Fehler untersuchen](https://docs.ptah.run/edge/operate/troubleshooting/).

Die Quellen der Website liegen in [`docs/site`](docs/site).
[`docs/README.md`](docs/README.md) verzeichnet auf Englisch die weiteren
Dokumente für Mitwirkende und zur Implementierung.

## Go-Pakete und Atlas-Kompatibilität

Go-Projekte können die dokumentierten Pakete einbetten, annotierte Structs als
Schemaquellen verwenden und `ptah-ls` für Editor-Unterstützung nutzen. Die
Einstiegspunkte sind das [Verzeichnis der öffentlichen API](https://docs.ptah.run/edge/extend/public-api/),
die [wiederverwendbaren Komponenten](https://docs.ptah.run/edge/extend/components/)
und die [Go-Annotationen](https://docs.ptah.run/edge/schema/go-annotations/).

Das separate Programm `ptah-compat` bietet Atlas-kompatible Befehle. Die native
`ptah`-Befehlsstruktur verwendet keine Atlas-Befehlspfade. Ptah beansprucht keine
vollständige Atlas-Parität. Die
[Kompatibilitätsübersicht](https://docs.ptah.run/edge/atlas/overview/)
und die [Konformitätsergebnisse](https://docs.ptah.run/edge/atlas/conformance/)
beschreiben den gemessenen Umfang und die Unterschiede.

## Lizenz und Hilfe

Ptah ist eine unabhängige Clean-Room-Implementierung unter der
[MIT-Lizenz](LICENSE). Sie verwendet keinen Atlas-Quellcode und ist weder mit
Ariga verbunden noch von Ariga empfohlen. Die
[Lizenzabgrenzung](https://docs.ptah.run/edge/atlas/license-boundary/)
dokumentiert die Herkunftsregeln.

Die Datenbanklogos oben kennzeichnen ausschließlich die unterstützten
Datenbankmotoren. Sie gehören ihren jeweiligen Eigentümern, die Ptah weder
empfehlen noch sponsern. [NOTICE](NOTICE) nennt die Herkunft und Lizenz jeder Datei.

Fragen und Ideen gehören in die
[Discussions](https://github.com/stokaro/ptah/discussions), Fehlerberichte und
Funktionswünsche in den [Issue-Tracker](https://github.com/stokaro/ptah/issues).
[CONTRIBUTING.md](CONTRIBUTING.md) beschreibt die Anforderungen an Berichte und
Änderungen. Für die Teilnahme gilt der [Verhaltenskodex](CODE_OF_CONDUCT.md).
Diese Ressourcen sind auf Englisch. Geschäftliche Anfragen gehen an
`ask <at> stokaro.com`.
