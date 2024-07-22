import os

import pandas as pd


def getTapDataParsed(part_id, raw_data_dir='.', device_type='Phone'):
    """
    Retrieves Tap Data relative to a participation ID (in input) that has been downloaded by taps.ai. This functions
    looks in every .parquet file in the data dump folder, retrieves all the devices associated with the participation
    ID given, removes duplicates and returns  all the information associated with the participation ID.

    input:
        - device_type      Can be one of {"Phone", "Tablet"} allows to
                           separately select the device type when the same
                           participation ID has been use for phones and tablets.
        - raw_data_dir     Where the parquet files downloaded from taps.ai are located. Default is '.', can also be a
                           top folder

    output:
        - taps:   contains all the tap data organized in session i.e. each
                  row represents all o the info between a screen-on and a screen-off
                  event. Each row contains also the devPartId. This is
                  important to match the correct appId to its app label and
                  category (given that multiple devices can be associated with
                  the same partId and difference devices have different app lists)
        - apps:   contains a table with all the apps of all the devices under
                  the same partId. The table contains appIds corresponding to
                  the ones present in the tap data but also app categories,
                  both the Play STore category ("category") and the QA
                  categories.
        - health: contains a table relative to the battery logs.
        - notes:  contains the notes associated with each device registered with
                  the given participation ID. The Table headers are self-explanatory
        - extras: table with one row for device that had registered with the
                  given partId and that has been concatenated. The table
                  contains. Table headers are self-explanatory.
    """

    tds = pd.DataFrame([])
    for path, subdirs, files in os.walk(raw_data_dir):
        for name in files:
            if 'MetaData' in name:
                meta_data = pd.read_parquet(os.path.join(path, name))
                for i, dev in meta_data[meta_data['partId'] == part_id].iterrows():
                    if 'deviceInfo' in dev:
                        if device_type == dev.deviceInfo['deviceType']:
                            tds = tds.append(dev)
    print(f"Found {len(tds)} device(s) of type {device_type}")

    # load actual stuff
    taps = pd.DataFrame([])
    apps = pd.DataFrame([])
    health = pd.DataFrame([])
    notes = pd.DataFrame([])
    extras = tds

    for i, dev in tds.iterrows():
        for path, subdirs, files in os.walk(raw_data_dir):
            for name in files:
                if dev.tapDeviceId in name:
                    if 'tapDataParsed' in name:
                        _taps = pd.read_parquet(os.path.join(path, name))
                        if "date" in _taps:
                            _taps = _taps.drop(columns="date")
                        taps = pd.concat([taps, _taps])
                    if 'deviceApplications' in name:
                        _apps = pd.read_parquet(os.path.join(path, name))
                        apps = pd.concat([apps, _apps])
                    if 'deviceNotes' in name:
                        _notes = pd.read_parquet(os.path.join(path, name))
                        notes = pd.concat([notes, _notes])
                    if 'deviceHealth' in name:
                        _health = pd.read_parquet(os.path.join(path, name))
                        health = pd.concat([health, _health])

    return taps.sort_values("start").reset_index(drop=True), apps.reset_index(), health.reset_index(), notes.reset_index(), extras.reset_index()
