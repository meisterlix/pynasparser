import datetime
import re
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
from lxml import etree

ID_NAMESPACE = "urn:adv:oid:"
CRS_NAMESPACE = "urn:adv:crs:"


@dataclass
class AX_Extract:
    xml_bytes: Optional[BytesIO]

    namespaces: Optional[dict] = field(init=False)
    xml_root: Optional[etree._Element] = field(init=False)
    crs_name: Optional[str] = field(init=False)
    current_crs: Optional[str] = field(init=False)
    gdf_buf: Optional[gpd.GeoDataFrame] = field(init=False)

    ax_flurstueck: Optional[gpd.GeoDataFrame] = field(init=False)
    ax_person: Optional[pd.DataFrame] = field(init=False)
    ax_buchungsblattbezirk: Optional[pd.DataFrame] = field(init=False)
    ax_buchungsblatt: Optional[pd.DataFrame] = field(init=False)
    ax_anschrift: Optional[pd.DataFrame] = field(init=False)
    ax_namensnummer: Optional[pd.DataFrame] = field(init=False)
    ax_buchungsstelle: Optional[pd.DataFrame] = field(init=False)

    def __post_init__(self):
        self.namespaces = self.get_namespaces()
        buf = self.xml_bytes
        self.xml_root = self.parse_xml_file(buf)
        self.crs_name = self.extract_original_crs_from_xml()
        self.current_crs = self.crs_name
        self.gdf_buf = self.get_flurstueck_geometry(buf)
        self.xml_bytes = None
        self.remove_broken_members()

        self.ax_flurstueck = self.get_ax_flurstueck_data()
        self.ax_person = self.get_ax_person_data()
        self.ax_buchungsblattbezirk = self.get_ax_buchungsblattbezirk_data()
        self.ax_buchungsblatt = self.get_ax_buchungsblatt_data()
        self.ax_anschrift = self.get_ax_anschrift_data()
        self.ax_namensnummer = self.get_ax_namensnummer_data()
        self.ax_buchungsstelle = self.get_ax_buchungsstelle_data()

        self.gdf_buf = None

    @staticmethod
    def remove_gml_id_prefix(string) -> str:
        if string.startswith(ID_NAMESPACE):
            return string.replace(ID_NAMESPACE, "")
        else:
            return string

    @staticmethod
    def remove_crs_prefix(string) -> str:
        if string.startswith(CRS_NAMESPACE):
            return string.replace(CRS_NAMESPACE, "")
        else:
            return string

    def get_namespaces(self) -> dict:
        self.xml_bytes.seek(0)
        raw_namespaces = dict(
            [
                node
                for event, node in etree.iterparse(self.xml_bytes, events=("start-ns",))
            ]
        )
        self.xml_bytes.seek(0)

        # Optionally normalize 'ax'
        target_ax_uri_list = [
            "http://www.adv-online.de/namespaces/adv/gid/7.1",
            "http://www.adv-online.de/namespaces/adv/gid/6.0",
        ]
        namespaces = {k: v for k, v in raw_namespaces.items() if k}

        default_uri = raw_namespaces.get("")
        if default_uri in target_ax_uri_list:
            namespaces["ax"] = default_uri

        for prefix, uri in list(namespaces.items()):
            if uri in target_ax_uri_list:
                namespaces["ax"] = uri

        return namespaces

    def parse_xml_file(xml_file: BytesIO):
        # Create parser with namespace and error handling

        xml_file.seek(0)
        parser = etree.XMLParser(
            remove_blank_text=True, encoding="utf-8", ns_clean=True, recover=True
        )

        # Parse the file
        try:
            tree = etree.parse(xml_file, parser)
            return tree.getroot()
        except etree.XMLSyntaxError as e:
            print(f"Error parsing XML file: {e}")

            raise

    @staticmethod
    def format_crs_name(crs_string: str) -> str:
        # Example input: "ETRS89_UTM33"
        # Desired output: "ETRS89 / UTM zone 33N"
        pattern = re.compile(r"^ETRS89_UTM(\d+)$", re.IGNORECASE)
        match = pattern.match(crs_string)
        if not match:
            raise ValueError(
                f"String {crs_string!r} is not in the expected format 'ETRS89_UTM<number>'"
            )
        zone = match.group(1)
        return f"ETRS89 / UTM zone {zone}N"

    @staticmethod
    def parse_xml_file(xml_file: BytesIO):
        # Create parser with namespace and error handling

        xml_file.seek(0)
        parser = etree.XMLParser(
            remove_blank_text=True, encoding="utf-8", ns_clean=True, recover=True
        )

        # Parse the file
        try:
            tree = etree.parse(xml_file, parser)
            return tree.getroot()
        except etree.XMLSyntaxError as e:
            print(f"Error parsing XML file: {e}")

            raise

    @staticmethod
    def find_elements(
        root: etree._Element, xpath, namespaces
    ) -> list[etree._Element] | None:
        """
        Find elements using XPath with namespace support

        Args:
            root (lxml.etree._Element): Root XML element
            xpath (str): XPath expression
            namespaces (dict, optional): Namespace dictionary

        Returns:
            list: List of matching elements
        """
        elements: list = root.xpath(xpath, namespaces=namespaces)  # type: ignore
        if elements:
            return elements
        else:
            return None

    def extract_original_crs_from_xml(self) -> str | None:
        aa_koordinatenreferenzsystemangaben: list | None = self.find_elements(
            self.xml_root,
            "//ax:koordinatenangaben/ax:AA_Koordinatenreferenzsystemangaben",
            namespaces=self.namespaces,
        )
        if aa_koordinatenreferenzsystemangaben:
            for angabe in aa_koordinatenreferenzsystemangaben:
                standard = angabe.findtext("ax:standard", namespaces=self.namespaces)
                if standard != "true":
                    continue

                crs_etrs = angabe.find("ax:crs", namespaces=self.namespaces).attrib.get(
                    "{http://www.w3.org/1999/xlink}href"
                )

                try:
                    original_crs: str = AX_Extract.format_crs_name(
                        AX_Extract.remove_crs_prefix(crs_etrs)
                    )
                except Exception as e:
                    raise ValueError(
                        f"error when formatting extracted crs: {crs_etrs}: \n{e}"
                    )

                return original_crs
        else:
            return None

    @staticmethod
    def find_attr(
        element: etree._Element,
        path: str,
        attr: str,
        namespaces: dict[str, str],
        default: str | None = None,
    ) -> str | None:
        """
        Find and return the value of an attribute on a descendant XML element.

        This function searches for the first element matching the given XPath-like
        `path` (using `Element.find()` under the hood), and if found, returns the
        specified attribute's value. If either the element or the attribute does not
        exist, the provided `default` value is returned instead.

        Args:
            element: The XML element to search within.
            path: A tag name or relative XPath expression to locate the child element.
            attr: The fully qualified attribute name (including namespace if needed),
                e.g. "{http://www.w3.org/1999/xlink}href".
            namespaces: A dictionary mapping XML namespace prefixes to URIs, used to
                        resolve namespaced paths and attributes.
            default: The value to return if the element or attribute is not found.
                    Defaults to None.

        Returns:
            The value of the specified attribute as a string, or the `default`
            value if not found.

        Example:
            >>> find_attr(person, "ax:hat", "{http://www.w3.org/1999/xlink}href", namespaces)
            'urn:uuid:1234-5678-90ab-cdef'

        """
        el = element.find(path, namespaces=namespaces)
        return el.get(attr) if el is not None else default

    def remove_broken_members(self) -> None:
        """
        Remove wfs:member / gml:featureMember elements that contain
        <gml:pos> elements with only a single coordinate.
        Operates in-place on self.xml_root.
        """
        root = self.xml_root
        if root is None:
            return  # or raise, depending on how strict you want to be

        # Try wfs:member first
        members = root.xpath(
            "//ax:enthaelt/wfs:FeatureCollection/wfs:member",
            namespaces=self.namespaces,
        )
        # Fallback: gml:featureMember
        if not members:
            members = root.xpath(
                "//ax:enthaelt/wfs:FeatureCollection/gml:featureMember",
                namespaces=self.namespaces,
            )

        for member in members:
            pos_elements = member.xpath(".//gml:pos", namespaces=self.namespaces)

            for pos in pos_elements:
                # pos.text might be None, so guard for that
                if pos.text is None:
                    continue

                coordinates = pos.text.strip().split()

                # "broken" member: only a single coordinate
                if len(coordinates) == 1:
                    parent = member.getparent()
                    if parent is not None:
                        parent.remove(member)
                    break  # stop checking more <gml:pos> in this member

    def get_flurstueck_geometry(
        self, xml_bytesIO: BytesIO, layer_name: str = "AX_Flurstueck"
    ) -> gpd.GeoDataFrame:
        flurstuecke_gdf = gpd.read_file(xml_bytesIO, layer=layer_name, engine="pyogrio")
        flurstuecke_gdf = flurstuecke_gdf[["identifier", "geometry"]]
        flurstuecke_gdf = flurstuecke_gdf.rename(
            {"identifier": "ax_flurstueck_id"}, axis="columns"
        )
        flurstuecke_gdf["ax_flurstueck_id"] = (
            flurstuecke_gdf["ax_flurstueck_id"]
            .astype(str)
            .apply(AX_Extract.remove_gml_id_prefix)
        )

        flurstuecke_gdf.set_crs(self.crs_name, inplace=True)
        return flurstuecke_gdf

    @staticmethod
    def get_aa_lebenszeitintervall_beginnt(
        alkis_member: etree._Element, namespaces: list
    ) -> Optional[datetime.datetime]:
        """ISO 8601 UTC"""
        aa_lebenszeitintervall_beginnt = alkis_member.find(
            ".//ax:lebenszeitintervall/ax:AA_Lebenszeitintervall/ax:beginnt",
            namespaces=namespaces,  # type: ignore
        )
        if (
            aa_lebenszeitintervall_beginnt is not None
            and aa_lebenszeitintervall_beginnt.text
        ):
            try:
                aa_lebenszeitintervall_beginnt = datetime.datetime.fromisoformat(
                    aa_lebenszeitintervall_beginnt.text.replace("Z", "+00:00")
                )
                return aa_lebenszeitintervall_beginnt
            except ValueError as e:
                print(
                    f"{aa_lebenszeitintervall_beginnt.text} is not a valid datetime: {e}"
                )
                return None

    def get_ax_flurstueck_data(self) -> gpd.GeoDataFrame:
        alkis_id: str
        flurstueckskennzeichen: str | None
        amtliche_flaeche: float | None
        ax_buchungsstelle_id: str | None
        ax_lagebezeichnung_ohne_hausnummer_id: str | None
        ax_zeitpunkt_der_entstehung: str | None
        aa_lebenszeitintervall_beginnt: datetime.datetime | None

        flurstuecke_data = {}
        flurstuecke_data["ax_flurstueck_id"] = []
        flurstuecke_data["flurstueckskennzeichen"] = []
        flurstuecke_data["amtliche_flaeche"] = []
        flurstuecke_data["ax_buchungsstelle_id"] = []
        flurstuecke_data["ax_lagebezeichnung_ohne_hausnummer_id"] = []
        flurstuecke_data["zeitpunktDerEntstehung"] = []
        flurstuecke_data["aa_lebenszeitintervall_beginnt"] = []

        flurstuecke = AX_Extract.find_elements(
            self.xml_root,
            "//ax:enthaelt/wfs:FeatureCollection/wfs:member/ax:AX_Flurstueck",
            namespaces=self.namespaces,
        )
        if not flurstuecke:
            flurstuecke = AX_Extract.find_elements(
                self.xml_root,
                "//ax:enthaelt/wfs:FeatureCollection/gml:featureMember/ax:AX_Flurstueck",
                namespaces=self.namespaces,
            )
        if not flurstuecke:
            raise ValueError(
                "No AX_Flurstueck elements found in the provided XML file."
            )

        for flurstueck in flurstuecke:
            flurstueckskennzeichen = None
            amtliche_flaeche = None
            ax_buchungsstelle_id = None
            ax_lagebezeichnung_ohne_hausnummer_id = None
            ax_zeitpunkt_der_entstehung = None
            aa_lebenszeitintervall_beginnt = None

            gml_id = flurstueck.attrib.get("{http://www.opengis.net/gml/3.2}id")

            if not gml_id:
                print("No gml:id found for flurstueck, skipping...")
                continue
            alkis_id = gml_id
            flurstueckskennzeichen = flurstueck.findtext(
                "ax:flurstueckskennzeichen", namespaces=self.namespaces
            )
            amtliche_flaeche_text = flurstueck.findtext(
                "ax:amtlicheFlaeche", namespaces=self.namespaces
            )
            if amtliche_flaeche_text is not None:
                try:
                    amtliche_flaeche = float(amtliche_flaeche_text)
                except ValueError:
                    print(
                        f"Could not convert amtliche_flaeche '{amtliche_flaeche_text}' to float."
                    )
                    amtliche_flaeche = None

            ax_zeitpunkt_der_entstehung = flurstueck.findtext(
                "ax:zeitpunktDerEntstehung", namespaces=self.namespaces
            )

            aa_lebenszeitintervall_beginnt = (
                AX_Extract.get_aa_lebenszeitintervall_beginnt(
                    alkis_member=flurstueck,
                    namespaces=self.namespaces,  # type: ignore
                )
            )

            ist_gebucht_id = AX_Extract.find_attr(
                element=flurstueck,
                path="ax:istGebucht",
                attr="{http://www.w3.org/1999/xlink}href",
                namespaces=self.namespaces,
            )
            if ist_gebucht_id is not None:
                href_gebucht_value = AX_Extract.remove_gml_id_prefix(ist_gebucht_id)
                ax_buchungsstelle_id = href_gebucht_value

            zeigt_auf_id = AX_Extract.find_attr(
                element=flurstueck,
                path="ax:zeigtAuf",
                attr="{http://www.w3.org/1999/xlink}href",
                namespaces=self.namespaces,
            )

            if zeigt_auf_id is not None:
                href_zeigt_auf_value = AX_Extract.remove_gml_id_prefix(zeigt_auf_id)
                ax_lagebezeichnung_ohne_hausnummer_id = href_zeigt_auf_value

            flurstuecke_data["ax_flurstueck_id"].append(alkis_id)
            flurstuecke_data["flurstueckskennzeichen"].append(flurstueckskennzeichen)
            flurstuecke_data["amtliche_flaeche"].append(amtliche_flaeche)
            flurstuecke_data["ax_buchungsstelle_id"].append(ax_buchungsstelle_id)
            flurstuecke_data["ax_lagebezeichnung_ohne_hausnummer_id"].append(
                ax_lagebezeichnung_ohne_hausnummer_id
            )
            flurstuecke_data["zeitpunktDerEntstehung"].append(
                ax_zeitpunkt_der_entstehung
            )
            flurstuecke_data["aa_lebenszeitintervall_beginnt"].append(
                aa_lebenszeitintervall_beginnt
            )

        df = pd.DataFrame.from_dict(flurstuecke_data)

        gdf = self.gdf_buf.merge(df, on="ax_flurstueck_id")

        return gdf

    def get_ax_person_data(self) -> pd.DataFrame:
        identifier: str
        nachname_oder_firma: str | None = None
        vorname: str | None = None
        anrede: str | None = None
        namensbestandteil: str | None = None
        akademischer_grad: str | None = None
        geburtsname: str | None = None
        geburtsdatum: str | None = None
        anschrift_id: str | None = None
        aa_lebenszeitintervall_beginnt: datetime.datetime | None = None
        anlass: str | None = None

        personen_data: dict[str, list[Any]] = {}
        personen_data["ax_person_id"] = []
        personen_data["nachname_oder_firma"] = []
        personen_data["vorname"] = []
        personen_data["anrede"] = []
        personen_data["namensbestandteil"] = []
        personen_data["akademischer_grad"] = []
        personen_data["geburtsname"] = []
        personen_data["geburtsdatum"] = []
        personen_data["ax_anschrift_id"] = []
        personen_data["aa_lebenszeitintervall_beginnt"] = []
        personen_data["anlass"] = []

        personen = AX_Extract.find_elements(
            self.xml_root,
            "//ax:enthaelt/wfs:FeatureCollection/wfs:member/ax:AX_Person",
            namespaces=self.namespaces,
        )
        if not personen:
            personen = AX_Extract.find_elements(
                self.xml_root,
                "//ax:enthaelt/wfs:FeatureCollection/gml:featureMember/ax:AX_Person",
                namespaces=self.namespaces,
            )

        if not personen:
            raise ValueError("No AX_Person elements found in the provided XML file.")

        for person in personen:
            gml_id = person.attrib.get("{http://www.opengis.net/gml/3.2}id")
            if gml_id:
                identifier = gml_id
                # set to None as default to keep dict colums to same length for missing values
                nachname_oder_firma = None
                vorname = None
                anrede = None
                geburtsname = None
                geburtsdatum = None
                anschrift_id = None
                aa_lebenszeitintervall_beginnt = None
                anlass = None

                nachname_oder_firma = person.findtext(
                    "ax:nachnameOderFirma", namespaces=self.namespaces
                )
                vorname = person.findtext("ax:vorname", namespaces=self.namespaces)
                anrede = person.findtext("ax:anrede", namespaces=self.namespaces)
                geburtsname = person.findtext(
                    "ax:geburtsname", namespaces=self.namespaces
                )
                geburtsdatum = person.findtext(
                    "ax:geburtsdatum", namespaces=self.namespaces
                )
                anlass = person.findtext("ax:anlass", namespaces=self.namespaces)
                namensbestandteil = person.findtext(
                    "ax:namensbestandteil", namespaces=self.namespaces
                )
                akademischer_grad = person.findtext(
                    "ax:akademischerGrad", namespaces=self.namespaces
                )

                aa_lebenszeitintervall_beginnt = (
                    AX_Extract.get_aa_lebenszeitintervall_beginnt(
                        alkis_member=person,
                        namespaces=self.namespaces,  # type: ignore
                    )
                )
                anschrift_id = AX_Extract.find_attr(
                    element=person,
                    path="ax:hat",
                    attr="{http://www.w3.org/1999/xlink}href",
                    namespaces=self.namespaces,
                )
                if anschrift_id:
                    anschrift_id = AX_Extract.remove_gml_id_prefix(anschrift_id)

            personen_data["ax_person_id"].append(identifier)
            personen_data["nachname_oder_firma"].append(nachname_oder_firma)
            personen_data["vorname"].append(vorname)
            personen_data["anrede"].append(anrede)
            personen_data["namensbestandteil"].append(namensbestandteil)
            personen_data["akademischer_grad"].append(akademischer_grad)
            personen_data["geburtsname"].append(geburtsname)
            personen_data["geburtsdatum"].append(geburtsdatum)
            personen_data["ax_anschrift_id"].append(anschrift_id)
            personen_data["aa_lebenszeitintervall_beginnt"].append(
                aa_lebenszeitintervall_beginnt
            )
            personen_data["anlass"].append(anlass)

        df = pd.DataFrame.from_dict(personen_data)
        return df

    def get_ax_buchungsblattbezirk_data(self) -> pd.DataFrame:
        identifier: str | None
        schluessel_gesamt: str | None  # land_id + bezirk_id
        bezeichnung: str | None
        ax_dienststelle_schluessel_land: str | None
        ax_buchungsblattbezirk_schluessel_bezirk: str | None
        ax_dienststelle_schluessel_stelle: str | None
        ax_dienststelle_id: str | None  # land_id + stelle_id
        aa_lebenszeitintervall_beginnt: datetime.datetime | None = None
        anlass: str | None

        buchungsblattbezirk_data = {}
        buchungsblattbezirk_data["ax_buchungsblattbezirk_id"] = []
        buchungsblattbezirk_data["schluessel_gesamt"] = []
        buchungsblattbezirk_data["bbz_bezeichnung"] = []
        buchungsblattbezirk_data["ax_buchungsblattbezirk_schluessel_bezirk"] = []
        buchungsblattbezirk_data["ax_buchungsblattbezirk_schluessel_land"] = []
        buchungsblattbezirk_data["ax_dienststelle_schluessel_land"] = []
        buchungsblattbezirk_data["ax_dienststelle_schluessel_stelle"] = []
        buchungsblattbezirk_data["gehoert_zu_ax_dienststelle_schluessel_stelle"] = []
        buchungsblattbezirk_data["aa_lebenszeitintervall_beginnt"] = []
        buchungsblattbezirk_data["anlass"] = []

        buchungsblattbezirke = AX_Extract.find_elements(
            self.xml_root,
            "//ax:enthaelt/wfs:FeatureCollection/wfs:member/ax:AX_Buchungsblattbezirk",
            namespaces=self.namespaces,
        )
        if not buchungsblattbezirke:
            buchungsblattbezirke = AX_Extract.find_elements(
                self.xml_root,
                "//ax:enthaelt/wfs:FeatureCollection/gml:featureMember/ax:AX_Buchungsblattbezirk",
                namespaces=self.namespaces,
            )

        if not buchungsblattbezirke:
            raise ValueError(
                "No AX_Buchungsblattbezirk elements found in the provided XML file."
            )
        for buchungsblattbezirk in buchungsblattbezirke:
            identifier = None
            schluessel_gesamt = None
            bezeichnung = None
            ax_dienststelle_schluessel_land = None
            ax_dienststelle_schluessel_stelle = None
            ax_buchungsblattbezirk_schluessel_bezirk = None
            ax_buchungsblattbezirk_schluessel_land = None
            dienststelle = None
            buchungsblattbezirk_schluessel = None
            aa_lebenszeitintervall_beginnt = None
            anlass = None

            aa_lebenszeitintervall_beginnt = (
                AX_Extract.get_aa_lebenszeitintervall_beginnt(
                    alkis_member=buchungsblattbezirk,
                    namespaces=self.namespaces,  # type: ignore
                )
            )
            identifier = buchungsblattbezirk.attrib.get(
                "{http://www.opengis.net/gml/3.2}id"
            )
            schluessel_gesamt = buchungsblattbezirk.findtext(
                "ax:schluesselGesamt", namespaces=self.namespaces
            )
            anlass = buchungsblattbezirk.findtext(
                "ax:anlass", namespaces=self.namespaces
            )
            bezeichnung = buchungsblattbezirk.findtext(
                "ax:bezeichnung", namespaces=self.namespaces
            )
            buchungsblattbezirk_schluessel = buchungsblattbezirk.find(
                "ax:schluessel/ax:AX_Buchungsblattbezirk_Schluessel",
                namespaces=self.namespaces,
            )
            if buchungsblattbezirk_schluessel is not None:
                for child in buchungsblattbezirk_schluessel:
                    if child.tag.endswith("land"):
                        ax_buchungsblattbezirk_schluessel_land = child.text
                    if child.tag.endswith("bezirk"):
                        ax_buchungsblattbezirk_schluessel_bezirk = child.text

            dienststelle = buchungsblattbezirk.find(
                "ax:gehoertZu/ax:AX_Dienststelle_Schluessel", namespaces=self.namespaces
            )
            if dienststelle is not None:
                for child in dienststelle:
                    if child.tag.endswith("land"):
                        ax_dienststelle_schluessel_land = child.text
                    if child.tag.endswith("stelle"):
                        ax_dienststelle_schluessel_stelle = child.text

            buchungsblattbezirk_data["ax_buchungsblattbezirk_id"].append(identifier)
            buchungsblattbezirk_data["schluessel_gesamt"].append(schluessel_gesamt)
            buchungsblattbezirk_data["bbz_bezeichnung"].append(bezeichnung)
            buchungsblattbezirk_data["ax_buchungsblattbezirk_schluessel_land"].append(
                ax_buchungsblattbezirk_schluessel_land
            )
            buchungsblattbezirk_data["ax_buchungsblattbezirk_schluessel_bezirk"].append(
                ax_buchungsblattbezirk_schluessel_bezirk
            )
            buchungsblattbezirk_data[
                "gehoert_zu_ax_dienststelle_schluessel_stelle"
            ].append(ax_dienststelle_schluessel_stelle)
            buchungsblattbezirk_data["ax_dienststelle_schluessel_land"].append(
                ax_dienststelle_schluessel_land
            )
            buchungsblattbezirk_data["ax_dienststelle_schluessel_stelle"].append(
                ax_dienststelle_schluessel_stelle
            )
            buchungsblattbezirk_data["aa_lebenszeitintervall_beginnt"].append(
                aa_lebenszeitintervall_beginnt
            )
            buchungsblattbezirk_data["anlass"].append(anlass)

        df = pd.DataFrame.from_dict(buchungsblattbezirk_data)
        return df

    def get_ax_buchungsblatt_data(self) -> pd.DataFrame:
        # TODO: add ax_buchungsblattbezirk_schluessel_bezirk & ax_buchungsblattbezirk_schluessel_land (optional)
        identifier: str | None
        buchungsblattkennzeichen: str | None
        buchungsblattbezirk_land: str | None
        buchungsblattnummer__mit_buchstabenerweiterung: str | None
        blattart: str | None
        aa_lebenszeitintervall_beginnt: datetime.datetime | None
        anlass: str | None

        buchungsblatt_data = {}
        buchungsblatt_data["ax_buchungsblatt_id"] = []
        buchungsblatt_data["buchungsblattkennzeichen"] = []
        buchungsblatt_data["land"] = []
        buchungsblatt_data["bezirk"] = []
        buchungsblatt_data["schluessel_gesamt"] = []
        buchungsblatt_data["blattart"] = []
        buchungsblatt_data["buchungsblattnummer_mit_buchstabenerweiterung"] = []
        buchungsblatt_data["aa_lebenszeitintervall_beginnt"] = []
        buchungsblatt_data["anlass"] = []

        buchungsblätter = AX_Extract.find_elements(
            self.xml_root,
            "//ax:enthaelt/wfs:FeatureCollection/wfs:member/ax:AX_Buchungsblatt",
            namespaces=self.namespaces,
        )

        if not buchungsblätter:
            buchungsblätter = AX_Extract.find_elements(
                self.xml_root,
                "//ax:enthaelt/wfs:FeatureCollection/gml:featureMember/ax:AX_Buchungsblatt",
                namespaces=self.namespaces,
            )

        if not buchungsblätter:
            raise ValueError(
                "No AX_Buchungsblatt elements found in the provided XML file."
            )

        for buchungsblatt in buchungsblätter:
            identifier = None
            buchungsblattkennzeichen = None
            buchungsblattbezirk_bezirk = None
            buchungsblattbezirk_land = None
            schluesselgesamt = None
            buchungsblattnummer__mit_buchstabenerweiterung = None
            blattart = None
            anlass = None
            aa_lebenszeitintervall_beginnt = None

            identifier = buchungsblatt.attrib.get("{http://www.opengis.net/gml/3.2}id")
            buchungsblattkennzeichen = buchungsblatt.findtext(
                "ax:buchungsblattkennzeichen", namespaces=self.namespaces
            )
            buchungsblattnummer__mit_buchstabenerweiterung = buchungsblatt.findtext(
                "ax:buchungsblattnummerMitBuchstabenerweiterung",
                namespaces=self.namespaces,
            )
            blattart = buchungsblatt.findtext("ax:blattart", namespaces=self.namespaces)
            anlass = buchungsblatt.findtext("ax:anlass", namespaces=self.namespaces)

            aa_lebenszeitintervall_beginnt = (
                AX_Extract.get_aa_lebenszeitintervall_beginnt(
                    alkis_member=buchungsblatt,
                    namespaces=self.namespaces,  # type: ignore
                )
            )
            ax_buchungsblattbezirk_schluessel = buchungsblatt.find(
                "ax:buchungsblattbezirk/ax:AX_Buchungsblattbezirk_Schluessel",
                namespaces=self.namespaces,
            )
            if ax_buchungsblattbezirk_schluessel is not None:
                for child in ax_buchungsblattbezirk_schluessel:
                    if child.tag.endswith("land"):
                        buchungsblattbezirk_land = child.text
                    if child.tag.endswith("bezirk"):
                        buchungsblattbezirk_bezirk = child.text
                    if buchungsblattbezirk_land and buchungsblattbezirk_bezirk:
                        schluesselgesamt = (
                            buchungsblattbezirk_land + buchungsblattbezirk_bezirk
                        )

            buchungsblatt_data["ax_buchungsblatt_id"].append(identifier)
            buchungsblatt_data["buchungsblattkennzeichen"].append(
                buchungsblattkennzeichen
            )
            buchungsblatt_data["land"].append(buchungsblattbezirk_land)
            buchungsblatt_data["bezirk"].append(buchungsblattbezirk_bezirk)
            buchungsblatt_data["schluessel_gesamt"].append(schluesselgesamt)
            buchungsblatt_data["blattart"].append(blattart)
            buchungsblatt_data["buchungsblattnummer_mit_buchstabenerweiterung"].append(
                buchungsblattnummer__mit_buchstabenerweiterung
            )
            buchungsblatt_data["aa_lebenszeitintervall_beginnt"].append(
                aa_lebenszeitintervall_beginnt
            )
            buchungsblatt_data["anlass"].append(anlass)

        df = pd.DataFrame.from_dict(buchungsblatt_data)
        return df

    def get_ax_anschrift_data(self) -> pd.DataFrame:
        identifier: str | None
        ort_post: str | None
        postleitzahl_postzustellung: str | None
        strasse: str | None
        hausnummer: str | None
        ortsteil: str | None
        aa_lebenszeitintervall_beginnt: datetime.datetime | None
        anlass: str | None
        tel: str | None
        weitere_adressen: str | None

        anschrift_data = {}
        anschrift_data["ax_anschrift_id"] = []
        anschrift_data["ort_post"] = []
        anschrift_data["postleitzahl_postzustellung"] = []
        anschrift_data["strasse"] = []
        anschrift_data["hausnummer"] = []
        anschrift_data["ortsteil"] = []
        anschrift_data["aa_lebenszeitintervall_beginnt"] = []
        anschrift_data["anlass"] = []
        anschrift_data["tel"] = []
        anschrift_data["weitere_adressen"] = []

        anschriften = AX_Extract.find_elements(
            self.xml_root,
            "//ax:enthaelt/wfs:FeatureCollection/wfs:member/ax:AX_Anschrift",
            namespaces=self.namespaces,
        )
        if not anschriften:
            anschriften = AX_Extract.find_elements(
                self.xml_root,
                "//ax:enthaelt/wfs:FeatureCollection/gml:featureMember/ax:AX_Anschrift",
                namespaces=self.namespaces,
            )

        if not anschriften:
            raise ValueError("No AX_Anschrift elements found in the provided XML file.")

        for anschrift in anschriften:
            identifier = None
            gml_id = None
            ort_post = None
            postleitzahl_postzustellung = None
            strasse = None
            hausnummer = None
            ortsteil = None
            aa_lebenszeitintervall_beginnt = None
            anlass = None
            tel = None
            weitere_adressen = None

            gml_id = anschrift.attrib.get("{http://www.opengis.net/gml/3.2}id")
            if not gml_id:
                print("No gml:id found for namensnummer, skipping...")
                continue
            identifier = gml_id

            ortsteil = anschrift.findtext("ax:ortsteil", namespaces=self.namespaces)
            anlass = anschrift.findtext("ax:anlass", namespaces=self.namespaces)
            tel = anschrift.findtext("ax:TEL", namespaces=self.namespaces)
            weitere_adressen = anschrift.findtext(
                "ax:weitereAdressen", namespaces=self.namespaces
            )

            aa_lebenszeitintervall_beginnt = (
                AX_Extract.get_aa_lebenszeitintervall_beginnt(
                    alkis_member=anschrift,
                    namespaces=self.namespaces,  # type: ignore
                )
            )

            ort_post = anschrift.findtext("ax:ort_Post", namespaces=self.namespaces)

            postleitzahl_postzustellung = anschrift.findtext(
                "ax:postleitzahlPostzustellung", namespaces=self.namespaces
            )

            strasse = anschrift.findtext("ax:strasse", namespaces=self.namespaces)

            hausnummer = anschrift.findtext("ax:hausnummer", namespaces=self.namespaces)

            anschrift_data["ax_anschrift_id"].append(identifier)
            anschrift_data["ort_post"].append(ort_post)
            anschrift_data["postleitzahl_postzustellung"].append(
                postleitzahl_postzustellung
            )
            anschrift_data["strasse"].append(strasse)
            anschrift_data["hausnummer"].append(hausnummer)
            anschrift_data["aa_lebenszeitintervall_beginnt"].append(
                aa_lebenszeitintervall_beginnt
            )
            anschrift_data["ortsteil"].append(ortsteil)
            anschrift_data["anlass"].append(anlass)
            anschrift_data["tel"].append(tel)
            anschrift_data["weitere_adressen"].append(weitere_adressen)

        df = pd.DataFrame.from_dict(anschrift_data)
        return df

    def get_ax_namensnummer_data(self) -> pd.DataFrame:
        ax_person_id: str | None  # tag xlink:href benennt | optional
        laufende_nummer: str | None
        ax_buchungsblatt_id: str | None  # tag xlink:href istBestandteilVon
        anlass: str | None  # tag xlink:title
        art_der_rechtsgemeinschaft: str | None
        besteht_aus_rechtsverhaeltnissen_zu: str | None
        ax_anteil: float

        namensnummern_data = {}
        namensnummern_data["ax_namensnummer_id"] = []
        namensnummern_data["ax_person_id"] = []
        namensnummern_data["laufende_nummer"] = []
        namensnummern_data["ax_buchungsblatt_id"] = []
        namensnummern_data["anlass"] = []
        namensnummern_data["art_der_rechtsgemeinschaft"] = []
        namensnummern_data["besteht_aus_rechtsverhaeltnissen_zu"] = []
        namensnummern_data["anteil"] = []

        namensnummern = AX_Extract.find_elements(
            self.xml_root,
            "//ax:enthaelt/wfs:FeatureCollection/wfs:member/ax:AX_Namensnummer",
            namespaces=self.namespaces,
        )
        if not namensnummern:
            namensnummern = AX_Extract.find_elements(
                self.xml_root,
                "//ax:enthaelt/wfs:FeatureCollection/gml:featureMember/ax:AX_Namensnummer",
                namespaces=self.namespaces,
            )

        if not namensnummern:
            raise ValueError(
                "No AX_Namensnummer elements found in the provided XML file."
            )

        for namensnummer in namensnummern:
            ax_person_id = None
            laufende_nummer = None
            ax_buchungsblatt_id = None
            benennt_id = None
            anlass = None
            art_der_rechtsgemeinschaft = None
            besteht_aus_rechtsverhaeltnissen_zu = None
            ax_anteil = float(1)
            anteil_zaehler = None
            anteil_nenner = None
            laufende_nummer = None

            gml_id = namensnummer.attrib.get("{http://www.opengis.net/gml/3.2}id")
            if not gml_id:
                print("No gml:id found for namensnummer, skipping...")
                continue
            identifier = gml_id

            ist_bestandteil_von_id = AX_Extract.find_attr(
                element=namensnummer,
                path="ax:istBestandteilVon",
                attr="{http://www.w3.org/1999/xlink}href",
                namespaces=self.namespaces,
            )
            if ist_bestandteil_von_id is not None:
                href_bestandteil_value = AX_Extract.remove_gml_id_prefix(
                    ist_bestandteil_von_id
                )
                ax_buchungsblatt_id = href_bestandteil_value

            laufende_nummer = namensnummer.findtext(
                "ax:laufendeNummerNachDIN1421", namespaces=self.namespaces
            )

            benennt_id = AX_Extract.find_attr(
                element=namensnummer,
                path="ax:benennt",
                attr="{http://www.w3.org/1999/xlink}href",
                namespaces=self.namespaces,
            )
            if benennt_id is not None:
                href_benennt_value = AX_Extract.remove_gml_id_prefix(benennt_id)
                ax_person_id = href_benennt_value

            anlass = AX_Extract.find_attr(
                element=namensnummer,
                path="ax:anlass",
                attr="{http://www.w3.org/1999/xlink}title",
                namespaces=self.namespaces,
            )

            art_der_rechtsgemeinschaft = namensnummer.findtext(
                "ax:artDerRechtsgemeinschaft", namespaces=self.namespaces
            )

            besteht_aus_rechtsverhaeltnissen_zu = AX_Extract.find_attr(
                element=namensnummer,
                path="ax:bestehtAusRechtsverhaeltnissenZu",
                attr="{http://www.w3.org/1999/xlink}href",
                namespaces=self.namespaces,
            )
            if besteht_aus_rechtsverhaeltnissen_zu is not None:
                besteht_aus_rechtsverhaeltnissen_zu = AX_Extract.remove_gml_id_prefix(
                    besteht_aus_rechtsverhaeltnissen_zu
                )

            anteil_zaehler = namensnummer.findtext(
                "ax:anteil/ax:AX_Anteil/ax:zaehler", namespaces=self.namespaces
            )
            anteil_nenner = namensnummer.findtext(
                "ax:anteil/ax:AX_Anteil/ax:nenner", namespaces=self.namespaces
            )

            zaehler = None
            nenner = None
            if anteil_zaehler is not None and anteil_nenner is not None:
                zaehler = float(anteil_zaehler)
                nenner = float(anteil_nenner)
                ax_anteil = zaehler / nenner

            namensnummern_data["ax_namensnummer_id"].append(identifier)
            namensnummern_data["ax_person_id"].append(ax_person_id)
            namensnummern_data["laufende_nummer"].append(laufende_nummer)
            namensnummern_data["ax_buchungsblatt_id"].append(ax_buchungsblatt_id)
            namensnummern_data["anlass"].append(anlass)
            namensnummern_data["art_der_rechtsgemeinschaft"].append(
                art_der_rechtsgemeinschaft
            )
            namensnummern_data["besteht_aus_rechtsverhaeltnissen_zu"].append(
                besteht_aus_rechtsverhaeltnissen_zu
            )
            namensnummern_data["anteil"].append(ax_anteil)

        df = pd.DataFrame.from_dict(namensnummern_data)
        return df

    def get_ax_buchungsstelle_data(self) -> pd.DataFrame:
        identifier: str
        buchungsart: str | None
        laufende_nummer: str | None
        ax_buchungsblatt_id: str | None

        buchungsstellen_data = {}
        buchungsstellen_data["ax_buchungsstelle_id"] = []
        buchungsstellen_data["buchungsart"] = []
        buchungsstellen_data["laufende_nummer"] = []
        buchungsstellen_data["ax_buchungsblatt_id"] = []

        buchungsstellen = AX_Extract.find_elements(
            self.xml_root,
            "//ax:enthaelt/wfs:FeatureCollection/wfs:member/ax:AX_Buchungsstelle",
            namespaces=self.namespaces,
        )
        if not buchungsstellen:
            buchungsstellen = AX_Extract.find_elements(
                self.xml_root,
                "//ax:enthaelt/wfs:FeatureCollection/gml:featureMember/ax:AX_Buchungsstelle",
                namespaces=self.namespaces,
            )
        if not buchungsstellen:
            raise ValueError(
                "No AX_Buchungsstelle elements found in the provided XML file."
            )

        for buchungsstelle in buchungsstellen:
            gml_id = buchungsstelle.attrib.get("{http://www.opengis.net/gml/3.2}id")
            if not gml_id:
                print("No gml:id found for buchungsstelle, skipping...")
                continue

            identifier = gml_id
            buchungsart = buchungsstelle.findtext(
                "ax:buchungsart", namespaces=self.namespaces
            )
            laufende_nummer = buchungsstelle.findtext(
                "ax:laufendeNummer", namespaces=self.namespaces
            )

            ist_bestandteil_von_id = AX_Extract.find_attr(
                element=buchungsstelle,
                path="ax:istBestandteilVon",
                attr="{http://www.w3.org/1999/xlink}href",
                namespaces=self.namespaces,
            )

            if ist_bestandteil_von_id is not None:
                href_bestandteil_value = AX_Extract.remove_gml_id_prefix(
                    ist_bestandteil_von_id
                )
                ax_buchungsblatt_id = href_bestandteil_value

            buchungsstellen_data["ax_buchungsstelle_id"].append(identifier)
            buchungsstellen_data["buchungsart"].append(buchungsart)
            buchungsstellen_data["laufende_nummer"].append(laufende_nummer)
            buchungsstellen_data["ax_buchungsblatt_id"].append(ax_buchungsblatt_id)

        df = pd.DataFrame.from_dict(buchungsstellen_data)
        return df
