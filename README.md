# Polygon City CLI

## Requirements

1. npm and Node v5 (https://nodejs.org/dist/v5.2.0/node-v5.2.0.pkg)
2. Redis (http://redis.io/download#installation)
3. A Mapzen Elevation Service key (https://mapzen.com/developers)

## Installation

```bash
$ npm install polygon-city-cli@latest -g
```

## Usage

Processing a new CityGML file:

```bash
$ polygon-city -e <epsg_code> -m <mapzen_elevation_key> -o /path/to/obj/output/directory /path/to/cityGml/file.xml
```

Resuming previous unfinished jobs:

```bash
$ polygon-city resume
```

## Step-by-step example

1. Run `npm install polygon-city-cli -g`
2. [Start a local Redis server](http://redis.io/topics/quickstart#starting-redis) (using port 6379)
3. [Download some CityGML data from Linz, Austria](http://geo.data.linz.gv.at/katalog/geodata/3d_geo_daten_lod2/)
4. Take note of [the EPSG code used for the Linz dataset](http://geo.data.linz.gv.at/katalog/geodata/3d_geo_daten_lod2/Beschreibung.txt) (EPSG:31255)
5. Run the command line tool:

```bash
$ polygon-city -e 31255 -m "your_mapzen_key" -o /path/to/obj/output/directory /path/to/cityGml/file.xml
```
