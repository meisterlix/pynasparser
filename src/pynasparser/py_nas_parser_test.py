from io import BytesIO
from pathlib import Path

from pynasparser.py_nas_parser import AX_Extract

TEST_DATA_DIR = "test_data"


def get_xml_bytesIO(filepath: str) -> BytesIO:
    with open(filepath, "rb") as fh:
        buf = BytesIO(fh.read())
        return buf


def main():
    for xml_path in Path(TEST_DATA_DIR).glob("*.xml"):
        if xml_path.is_file():
            input_xml_bytes = get_xml_bytesIO(str(xml_path.absolute()))

            ax_extract = AX_Extract(input_xml_bytes)
            # ax_extract.ax_person.to_csv(
            #     f"ax_person_{xml_path.name}.csv", sep="|", index=False
            # )


if __name__ == "__main__":
    main()
