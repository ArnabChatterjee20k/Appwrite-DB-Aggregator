# Migration and validations

### Pull the full Appwrite project state (databases, collections, attributes, documents, functions, buckets, files)
`python migration_validator.py --pull --output prod_snapshot.json`

### Compare two pulled project states and print migration diff
`python migration_validator.py --compare --source new_prod_snapshot.json --destination new_stage_snapshot.json`

### With resume and checkpointing logic to resume from in between
`python migration_validator.py --pull --output des_snapshot.json --resume`

### seedin appwrite from the json
`python migration_validator.py --seed prod_snapshot.json`

# DB faker tool
`python db_faker.py --init-schema`

### Generate and upload 1000 docs/collection
`python db_faker.py --generate`

### Generate 500 posts only and save to file, no upload
`python db_faker.py --generate --count 500 --collections posts --dry-run --output posts.json`

### Upload from file later and compare
`python db_faker.py --compare --collections posts --output posts.json`

### Appwrite database schema generator
* Mainly focused on generating the string based data for databases
* Make sure to change the project settings on appwrite.json
### Usage
> Follow (https://appwrite.io/docs/tooling/command-line/installation#initialization)
1. Install appwrite cli 
```
npm install -g appwrite-cli
```

2. Link the project

3. Run
```
appwrite push
```
