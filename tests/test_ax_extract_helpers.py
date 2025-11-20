import datetime

import pytest

from pynasparser.py_nas_parser import CRS_NAMESPACE, ID_NAMESPACE, AX_Extract


def test_remove_gml_id_prefix_strips_namespace():
    input_id = f"{ID_NAMESPACE}foobar"
    assert AX_Extract.remove_gml_id_prefix(input_id) == "foobar"


def test_remove_gml_id_prefix_leaves_other_strings():
    assert AX_Extract.remove_gml_id_prefix("something_else") == "something_else"


def test_remove_crs_prefix_strips_namespace():
    input_crs = f"{CRS_NAMESPACE}ETRS89_UTM33"
    assert AX_Extract.remove_crs_prefix(input_crs) == "ETRS89_UTM33"


def test_remove_crs_prefix_leaves_other_strings():
    assert AX_Extract.remove_crs_prefix("EPSG:25833") == "EPSG:25833"


@pytest.mark.parametrize(
    "crs_str, expected",
    [
        ("ETRS89_UTM33", "ETRS89 / UTM zone 33N"),
        ("ETRS89_UTM32", "ETRS89 / UTM zone 32N"),
        ("etrs89_utm33", "ETRS89 / UTM zone 33N"),  # case-insensitive
    ],
)
def test_format_crs_name_valid(crs_str, expected):
    assert AX_Extract.format_crs_name(crs_str) == expected


def test_format_crs_name_invalid_raises():
    with pytest.raises(ValueError):
        AX_Extract.format_crs_name("ETRS89-UTM33")  # wrong pattern


def test_get_aa_lebenszeitintervall_beginnt_valid_xml():
    from lxml import etree

    xml = """
    <ax:AX_Flurstueck xmlns:ax="http://example.com/ax">
      <ax:lebenszeitintervall>
        <ax:AA_Lebenszeitintervall>
          <ax:beginnt>2024-10-27T12:34:56Z</ax:beginnt>
        </ax:AA_Lebenszeitintervall>
      </ax:lebenszeitintervall>
    </ax:AX_Flurstueck>
    """
    root = etree.fromstring(xml.encode("utf-8"))
    result = AX_Extract.get_aa_lebenszeitintervall_beginnt(
        root, namespaces={"ax": "http://example.com/ax"}
    )
    assert isinstance(result, datetime.datetime)
    assert result == datetime.datetime(
        2024, 10, 27, 12, 34, 56, tzinfo=datetime.timezone.utc
    )


def test_get_aa_lebenszeitintervall_beginnt_invalid_returns_none():
    from lxml import etree

    xml = """
    <ax:AX_Flurstueck xmlns:ax="http://example.com/ax">
      <ax:lebenszeitintervall>
        <ax:AA_Lebenszeitintervall>
          <ax:beginnt>not-a-datetime</ax:beginnt>
        </ax:AA_Lebenszeitintervall>
      </ax:lebenszeitintervall>
    </ax:AX_Flurstueck>
    """
    root = etree.fromstring(xml.encode("utf-8"))
    result = AX_Extract.get_aa_lebenszeitintervall_beginnt(
        root, namespaces={"ax": "http://example.com/ax"}
    )
    assert result is None
