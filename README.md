# Polygon City CLI

## Installation

```bash
$ npm install polygon-city-cli -g
```

## Usage

```bash
$ polygon-city -e <epsg_code> -m <mapzen_elevation_key> -o /path/to/obj/output/directory /path/to/cityGml/file.xml
```

## Step-by-step example

1. Run `npm install polygon-city-cli -g`
2. [Start a local Redis server](http://redis.io/topics/quickstart#starting-redis) (using port 6379)
3. [Download some CityGML data from Linz, Austria](http://geo.data.linz.gv.at/katalog/geodata/3d_geo_daten_lod2/)
4. Take note of [the EPSG code used for the Linz dataset](http://geo.data.linz.gv.at/katalog/geodata/3d_geo_daten_lod2/Beschreibung.txt) (EPSG:31255)
5. [Get a Mapzen Elevation key](https://mapzen.com/developers)
6. Run the command line tool&hellip;

```bash
$ polygon-city -e 31255 -m "your_mapzen_key" -o /path/to/obj/output/directory /path/to/cityGml/file.xml
```

Remember to exit the process when it's finished outputting files (eg. `ctrl + c`) otherwise the process will stay open indefinitely. This is a known limitation until we come up with an automated approach for knowing when everything has finished processing.
