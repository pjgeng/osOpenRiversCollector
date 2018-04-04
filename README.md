# osOpenRiversCollector
Process OS OpenRivers into a set of uniquely named GB watercourses.

The script is merely a stage in the processing of the dataset.

1. Download the OS OpenRivers Dataset from the [OS OpenData Download](https://www.ordnancesurvey.co.uk/opendatadownload/products.html) pages
2. Import the 'watercourselink' and 'hydronode' shapefiles into a postgres database
3. Change the script provided to suit your setup - depending on your setup you will likely need to change _schema, _nodes_table, _links_table and will possibly need to change _proc_table and _out_table
4. Connect to the database and run the script as an anonymous block (i.e. copy and paste the entire script provided)
5. Depending on your machine specification you may have time for a coffee, nap or a holiday until the script finishes
6. Export the data from the database in the format required

The script produces a clean and stripped down dataset containing only the named GB watercourses. Where multiple watercourses exist with the same name (e.g. River Avon) each unique river is provided with a unique identifier. The script does NOT geolocate these further than their contained geometry.

The script automatically fills in unnamed gaps between named links starting from the source and working its way downstream to the outflow. There are some anomalies that need manual fixing, especially on the River Thames, Humber and Severn, though these may well disappear or change based on the input data currency.

The script is provided unsupported and as-is with no public future updates planned. Commercial support options may be available on request.
