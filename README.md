# pynasparser

Dieses Repository beinhaltet ein kleines privates Projekt zum Parsen von xml-files im NAS-Format (definiert in der [GeoInfoDok](https://www.adv-online.de/GeoInfoDok/)).

Aktuell kann ein Objekt der dataclass [AX_Extract](src/pynasparser/py_nas_parser.py#L16) aus einer BytesIO-Repräsentation eines NAS-XML-files kreiert werden. Mithilfe der __post_init__ method werden dann die Objektarten ax_flurstueck, ax_person, ax_buchungsblattbezirk, ax_buchungsblatt, ax_anschrift, ax_namensnummer & ax_buchungsstelle ausgelesen und als fields der AX_Extract-Instanz als pandas.DataFrame oder geopandas.GeoDataFrame (ax_flurstueck) abgerufen werden. 