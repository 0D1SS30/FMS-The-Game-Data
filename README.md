# FMS - The Game (Public AI Models & Ratings Feed)

This public repository hosts the automatically updated pre-trained AI models (`Random Forest ML` + `Dixon-Coles Poisson`) and live club ratings for **FMS - The Game** (PC & Android):

- `data/fms_models_and_ratings.json`: Pre-trained decision trees, ELO ratings, and rolling 5/10 match metrics across 15 European leagues.
- `data/espn_matches_archive.json`: Incremental historical match archive across 15 leagues.
- `tools/update_models_and_ratings.py`: Automated updater script executed by GitHub Actions.
