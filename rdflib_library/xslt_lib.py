import lxml.etree as ET
from xml.dom import minidom

def xslt_transform_string(xml_str, xslt_file, out_file):
    dom = ET.fromstring(xml_str)
    xslt = ET.parse(xslt_file)
    transform = ET.XSLT(xslt)
    new_dom = transform(dom)
    out_xml = minidom.parseString(ET.tostring(new_dom)).toprettyxml()
    with open(out_file, "w") as f:
        f.write(out_xml)