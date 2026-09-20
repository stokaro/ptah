<p align="center"><img src="docs/site/src/assets/logo.svg" alt="Le logo Ptah : une pierre de couronnement ambrée au-dessus de deux rangées bleu ciel sur un carré sombre aux coins arrondis" width="72" height="72"></p>

<h1 align="center">Ptah</h1>

<p align="center">Gestion open source des modifications de bases de données : schémas et état d’inférence persistant.</p>

<p align="center"><a href="README.md">English</a> · <a href="README.ja.md">日本語</a> · <a href="README.de.md">Deutsch</a> · <strong>Français</strong></p>

<p align="center">
  <a href="https://github.com/stokaro/ptah/actions/workflows/go-unit-tests.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/go-unit-tests.yml?branch=master&label=tests&logo=github" alt="État des tests unitaires sur master"></a>
  <a href="https://github.com/stokaro/ptah/actions/workflows/go-integration-tests.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/go-integration-tests.yml?branch=master&label=integration&logo=github" alt="État des tests d’intégration sur master"></a>
  <a href="https://github.com/stokaro/ptah/actions/workflows/capability-matrix.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/capability-matrix.yml?branch=master&label=databases&logo=github" alt="État de la matrice des capacités sur master, qui teste chaque série de versions déclarée des bases de données"></a>
  <a href="https://github.com/stokaro/ptah/releases/latest"><img src="https://img.shields.io/github/v/release/stokaro/ptah?label=release&logo=github" alt="Tag de la dernière version publiée"></a>
  <a href="https://pkg.go.dev/ptah.run"><img src="https://pkg.go.dev/badge/ptah.run.svg" alt="Référence des paquets Go de ptah.run"></a>
  <a href="https://github.com/stokaro/ptah/blob/master/go.mod"><img src="https://img.shields.io/github/go-mod/go-version/stokaro/ptah?label=go%20%E2%89%A5&logo=go&logoColor=white" alt="Version minimale de Go permettant de compiler ce module, déclarée dans go.mod"></a>
  <a href="https://github.com/stokaro/ptah/blob/master/LICENSE"><img src="https://img.shields.io/github/license/stokaro/ptah?label=license&color=blue" alt="Licence : MIT"></a>
</p>

<p align="center"><a href="#installation">Installation</a> · <a href="https://docs.ptah.run/edge/start/quick-start/">Démarrage rapide (en anglais)</a> · <a href="https://docs.ptah.run/edge/inference/overview/">Migrations d’inférence (en anglais)</a> · <a href="https://docs.ptah.run/edge/">Documentation (en anglais)</a> · <a href="https://docs.ptah.run/edge/databases/support-matrix/">Bases prises en charge (en anglais)</a></p>

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

Ptah gère les modifications des schémas de bases de données et de l’état
d’inférence persistant. Pour les schémas, il compare le schéma souhaité à une
base existante, puis génère des migrations versionnées ou applique directement
un plan approuvé. Pour l’état d’inférence, il construit une génération candidate
à côté de la génération active, appelle un service externe d’embeddings,
vérifie le résultat et fait basculer les consommateurs avec une possibilité de retour.

La CLI fonctionne sans chaîne de compilation Go. Les mêmes composants de
planification sont également disponibles sous forme de paquets Go.

## Modifications de schéma

<p align="center"><img src="docs/site/src/assets/product-journeys.svg" alt="Les sources de schéma et une base existante produisent un plan à examiner, transformé en fichiers de migration versionnés ou appliqué directement. Les spécifications d’inférence et les lignes sources produisent une génération candidate, vérifiée avant la bascule. La génération active reste disponible pour un retour arrière." width="1000"></p>

Les deux approches utilisent le même modèle de comparaison et de planification.
La différence est de savoir si le SQL devient un artefact revu dans le système
de gestion de versions avant son exécution.

## État d’inférence persistant

Ptah orchestre la migration ; il n’exécute pas l’inférence. Il lit les lignes
sources, appelle le service externe et écrit lui-même la génération candidate.
La génération active reste intacte jusqu’à la vérification et à la bascule.

<p align="center"><img src="docs/site/src/assets/inference-generation-lifecycle.svg" alt="La génération d’inférence active continue de servir les requêtes pendant que Ptah prépare, remplit, synchronise, indexe et vérifie une candidate. La bascule active la candidate vérifiée. Un retour arrière peut rétablir la génération précédente conservée ; sa suppression est une opération distincte et destructive." width="1000"></p>

Le [guide des migrations d’inférence](https://docs.ptah.run/edge/inference/overview/)
(en anglais) couvre la spécification, le rattrapage des modifications concurrentes,
l’évaluation, les approbations, le retour arrière et la suppression des anciennes
générations.

> [!NOTE]
> Ptah est au stade pre-GA, avant la disponibilité générale. L’arborescence des
> commandes natives et l’API Go publique peuvent encore changer.

## Installation

Le programme d’installation choisit la version actuelle pour Linux, macOS ou
Windows, vérifie sa somme de contrôle et installe `ptah`, `ptah-compat` et
`ptah-ls` dans votre répertoire utilisateur.

```bash
curl -fsSL https://ptah.run/install.sh | sh
```

Dans PowerShell :

```powershell
irm https://ptah.run/install.ps1 | iex
```

Le [guide d’installation](https://docs.ptah.run/edge/start/install/)
(en anglais) couvre le choix d’une version précise, la vérification des
signatures, le téléchargement sans exécution et la compilation des sources.

## Essayer Ptah avec SQLite

Enregistrez ce schéma souhaité dans `schema.sql` :

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE
);
```

Affichez le SQL, appliquez-le à une base temporaire et vérifiez que la base
correspond au fichier :

```bash
ptah schema render --schema-file schema.sql --dialect sqlite
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --auto-approve
ptah schema drift --db-url "sqlite://app.db" --schema-file schema.sql
```

La sortie attendue contient :

```text
CREATE TABLE "users" (
```

La sortie attendue contient :

```text
Schema apply completed successfully.
```

La sortie attendue contient :

```text
No schema drift detected.
```

La dernière commande renvoie le code 0 si la base correspond au fichier,
ce qui permet de l’utiliser comme contrôle en CI. Supprimez `app.db` et
`schema.sql` une fois l’essai terminé.

> [!CAUTION]
> `--auto-approve` ignore la demande de confirmation. Une modification directe
> du schéma peut supprimer des objets absents du schéma souhaité. Utilisez cette
> option ici uniquement parce que `app.db` est une base de test jetable.

Pour un parcours complet avec sorties attendues et vérifications, suivez le
[tutoriel des modifications directes du schéma](https://docs.ptah.run/edge/start/quick-start-direct/)
ou le
[tutoriel des migrations versionnées](https://docs.ptah.run/edge/start/quick-start-migrations/)
(tous deux en anglais).

## Choisir comment appliquer les changements de schéma

| Approche | À choisir quand | Point de départ |
| --- | --- | --- |
| Migrations versionnées | Les fichiers SQL doivent passer en revue de code et figurer dans l’historique des déploiements | `ptah migrations generate` |
| Modifications directes du schéma | Le schéma souhaité fait autorité et le plan doit être examiné puis appliqué immédiatement | `ptah schema plan` |

Les sources de schéma peuvent être du SQL, YAML, HCL, DBML, des annotations Go,
des chargeurs externes ou une base existante. Les fonctionnalités couvertes
varient selon le moteur. Consultez la
[matrice de support](https://docs.ptah.run/edge/databases/support-matrix/)
(en anglais) et `ptah db capabilities --db-url <url>` pour la cible concernée.

## Explorer la documentation

Les guides suivants sont en anglais :

- [Choisir une approche](https://docs.ptah.run/edge/start/choose-a-workflow/)
  en comparant migrations versionnées et modifications directes du schéma.
- [Inspecter une base existante](https://docs.ptah.run/edge/direct/inspect/)
  ou [comparer les schémas et détecter les dérives](https://docs.ptah.run/edge/direct/compare-and-drift/).
- [Vérifier l’intégrité des migrations](https://docs.ptah.run/edge/versioned/integrity-and-safety/)
  ou [tester les migrations et les schémas](https://docs.ptah.run/edge/testing/migrations-and-schema/).
- [Visualiser](https://docs.ptah.run/edge/schema/visualize/)
  ou [exporter](https://docs.ptah.run/edge/schema/export/) un schéma.
- [Migrer l’état d’inférence persistant](https://docs.ptah.run/edge/inference/overview/)
  pendant qu’un service externe calcule les embeddings.
- [Consulter les commandes natives](https://docs.ptah.run/edge/reference/native-commands/)
  ou [diagnostiquer une erreur](https://docs.ptah.run/edge/operate/troubleshooting/).

Les sources du site se trouvent dans [`docs/site`](docs/site).
[`docs/README.md`](docs/README.md) répertorie les documents en anglais destinés
aux contributeurs et consacrés à l’implémentation.

## Paquets Go et compatibilité Atlas

Les projets Go peuvent intégrer les paquets documentés, utiliser des structs
annotées comme sources de schéma et exécuter `ptah-ls` pour l’assistance dans
l’éditeur. Commencez par le [registre de l’API publique](https://docs.ptah.run/edge/extend/public-api/),
les [composants réutilisables](https://docs.ptah.run/edge/extend/components/)
ou les [annotations Go](https://docs.ptah.run/edge/schema/go-annotations/)
(en anglais).

Le binaire distinct `ptah-compat` expose des commandes compatibles avec Atlas.
L’arborescence native de `ptah` n’utilise pas les chemins de commandes Atlas.
Ptah ne revendique pas une parité complète avec Atlas.
La [présentation de la compatibilité](https://docs.ptah.run/edge/atlas/overview/)
et les [résultats de conformité](https://docs.ptah.run/edge/atlas/conformance/)
(en anglais) précisent la couverture mesurée et les différences.

## Licence et aide

Ptah est une implémentation indépendante en salle blanche, publiée sous
[licence MIT](LICENSE). Elle n’utilise pas le code source d’Atlas et n’est
ni affiliée à Ariga ni approuvée par Ariga. La
[politique de séparation des licences](https://docs.ptah.run/edge/atlas/license-boundary/)
(en anglais) décrit les règles de provenance.

Les logos ci-dessus identifient uniquement les moteurs pris en charge par Ptah.
Chacun appartient à son propriétaire, qui n’approuve ni ne parraine Ptah.
[NOTICE](NOTICE) précise la provenance et la licence de chaque fichier.

Posez vos questions et partagez vos idées dans les
[Discussions](https://github.com/stokaro/ptah/discussions). Signalez les bugs
et proposez des fonctionnalités dans le
[suivi des issues](https://github.com/stokaro/ptah/issues).
[CONTRIBUTING.md](CONTRIBUTING.md) décrit les informations nécessaires à un
rapport exploitable et les contrôles requis pour une modification. La participation
est encadrée par le [code de conduite](CODE_OF_CONDUCT.md). Ces ressources sont
en anglais. Pour les demandes commerciales : `ask <at> stokaro.com`.
