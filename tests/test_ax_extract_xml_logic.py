from io import BytesIO

import geopandas as gpd
import pandas as pd
import pytest
from lxml import etree

from pynasparser.py_nas_parser import AX_Extract


@pytest.fixture
def simple_xml_bytes():
    xml = """
    <ax:enthaelt
        xmlns:ax="http://www.adv-online.de/namespaces/adv/gid/7.1"
        xmlns:wfs="http://www.opengis.net/wfs/2.0"
        xmlns:gml="http://www.opengis.net/gml/3.2">
      <wfs:FeatureCollection>
        <wfs:member>
          <ax:AX_Flurstueck gml:id="urn:adv:oid:123">
            <gml:pos>10 20</gml:pos>
          </ax:AX_Flurstueck>
        </wfs:member>
        <wfs:member>
          <ax:AX_Flurstueck gml:id="urn:adv:oid:999">
            <gml:pos>42</gml:pos>
          </ax:AX_Flurstueck>
        </wfs:member>
      </wfs:FeatureCollection>
    </ax:enthaelt>
    """
    return BytesIO(xml.encode("utf-8"))


def test_remove_broken_members_removes_single_coord_member(
    monkeypatch, simple_xml_bytes
):
    # Patch heavy methods to no-op or minimal
    monkeypatch.setattr(
        AX_Extract,
        "get_flurstueck_geometry",
        lambda self, buf, layer_name="AX_Flurstueck": gpd.GeoDataFrame(
            {"ax_flurstueck_id": []}
        ),
    )
    monkeypatch.setattr(
        AX_Extract,
        "extract_original_crs_from_xml",
        lambda self: "ETRS89 / UTM zone 33N",
    )
    monkeypatch.setattr(
        AX_Extract, "get_ax_flurstueck_data", lambda self: gpd.GeoDataFrame()
    )
    monkeypatch.setattr(AX_Extract, "get_ax_person_data", lambda self: pd.DataFrame())
    monkeypatch.setattr(
        AX_Extract, "get_ax_buchungsblattbezirk_data", lambda self: pd.DataFrame()
    )
    monkeypatch.setattr(
        AX_Extract, "get_ax_buchungsblatt_data", lambda self: pd.DataFrame()
    )
    monkeypatch.setattr(
        AX_Extract, "get_ax_anschrift_data", lambda self: pd.DataFrame()
    )
    monkeypatch.setattr(
        AX_Extract, "get_ax_namensnummer_data", lambda self: pd.DataFrame()
    )
    monkeypatch.setattr(
        AX_Extract, "get_ax_buchungsstelle_data", lambda self: pd.DataFrame()
    )

    ax = AX_Extract(xml_bytes=simple_xml_bytes)

    # Now inspect xml_root directly
    ns = ax.namespaces
    # There should now be only one AX_Flurstueck element (the valid one)
    flurstuecke = ax.xml_root.xpath("//ax:AX_Flurstueck", namespaces=ns)
    ids = [el.attrib.get("{http://www.opengis.net/gml/3.2}id") for el in flurstuecke]

    assert "urn:adv:oid:123" in ids
    assert "urn:adv:oid:999" not in ids
