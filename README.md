# DBT Project

Ce projet utilise DBT (Data Build Tool) pour transformer les données dans votre data warehouse.

## Structure du projet

```
├── analyses/         # Analyses SQL
├── macros/          # Macros réutilisables
├── models/          # Modèles SQL
├── seeds/           # Données de référence
├── snapshots/       # Snapshots pour le suivi des changements
├── tests/           # Tests personnalisés
├── dbt_project.yml  # Configuration du projet
└── README.md        # Documentation
```

## Installation

1. Installer DBT :
```bash
pip install dbt-core
```

2. Configurer votre profil DBT dans `~/.dbt/profiles.yml`

## Utilisation

Pour exécuter les modèles :
```bash
dbt run
```

Pour exécuter les tests :
```bash
dbt test
```

Pour générer la documentation :
```bash
dbt docs generate
dbt docs serve
``` 