<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:cim="http://iec.ch/TC57/2010/CIM-schema-cim15#">
    <xsl:strip-space elements="*"/>
    <xsl:output omit-xml-declaration="yes" indent="yes"/>
    <!-- This is a generic search replace of attribute values -->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>
    <!-- rename tag rdf:Description to the value contained in rdf:type child node -->
    <xsl:template match="rdf:Description">
        <xsl:variable name="vRep" select="concat('cim:',substring-after(rdf:type/@rdf:resource,'#'))"/>
        <xsl:element name="{$vRep}">
            <xsl:apply-templates select="node()|@*"/>
        </xsl:element>
    </xsl:template>
    <!-- remove rdf:type attribute-->
    <xsl:template match="rdf:type" />
    <!--change the attribute name from rdf:about to rdf:ID + remove namespace from ID name-->
    <xsl:template match="@rdf:about">
        <xsl:attribute name="rdf:ID">
            <xsl:value-of select="substring-after(.,'#')"/>
        </xsl:attribute>
    </xsl:template>
    <!-- remove namespace from rdf:resource name-->
    <xsl:template match="@rdf:resource">
        <xsl:attribute name="rdf:resource">
            <xsl:value-of select="concat('#',substring-after(.,'#'))"/>
        </xsl:attribute>
    </xsl:template>
</xsl:stylesheet>