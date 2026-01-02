# f1fabric

## Overview
I decided to create a fun Formula 1 demo dataset for showcasing the integration between Fabric Data Agents & Microsoft Copilot and/or AI Foundry.  There are a lot of datasets out there and this one is inspired by the initial work done by [Shubham Yadav](https://www.linkedin.com/in/shubham-utdallas/) a few years ago.  Unfortunately the data source he references is no longer available.  Thus, I did some searching and found the [kaggle - Formula 1 Race Data](https://www.kaggle.com/datasets/jtrotman/formula-1-race-data/data).  This dataset is updated after each F1 race and using the solution I created in this repo, you can easily update the medallion architecture on a weekly basis during race season.

To do so, perform the following:
1. Create a brand new Fabric Workspace.
2. Clone this repo to your GitHub environment.
3. Add your newly cloned GitHub repo to your Fabric Workspace environment via the Git integration under Workspace settings. For more on this topic, check out [Connect a workspace to a Git repo - GitHub Connect](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=azure-devops%2CGitHub%2Ccommit-to-git#connect-a-workspace-to-a-git-repo).
4. Within your Fabric Lakehouse, perform an Update under Source Control to pull the Lakehouse, Notebooks and Data Agent into your newly created workspace.  FOr more on this topic, check out [Basic concepts in Git integration - Commits and updates](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process?tabs=Azure%2Cazure-devops#commits-and-updates).


## Kaggle F1 Data
You will need to download the .zip file from the [kaggle - Formula 1 Race Data](https://www.kaggle.com/datasets/jtrotman/formula-1-race-data/data) web site.  It contains the following csv files.
- circuits.csv
- constructor_results.csv
- constructor_standings.csv
- constructors.csv
- driver_standings.csv
- drivers.csv
- lap_times.csv
- pit_stops.csv
- qualifying.csv
- races.csv
- results.csv
- seasons.csv
- sprint_results.csv
- status.csv

Once these are downloaded and extracted on your local machine, you will need to upload the csv files to Files section of your Lakehouse within Fabric so that they can be utilized by the Fabric Notebooks in the next section.
