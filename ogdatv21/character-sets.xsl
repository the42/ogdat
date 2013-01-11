<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.iana.org/assignments" version="1.0">
  <xsl:output method="text" omit-xml-declaration="yes" indent="yes"/>
  <xsl:strip-space elements="*"/>
  
  <xsl:template match="/xs:registry">
    <xsl:apply-templates select="//xs:record"/>
  </xsl:template>

  <xsl:template match="xs:record">
    <xsl:value-of select="xs:name"/>
    <xsl:text>&#xA;</xsl:text>
    <xsl:apply-templates select="xs:preferred_alias"/>
    <xsl:apply-templates select="xs:alias"/>
  </xsl:template>

<xsl:template match="xs:preferred_alias">
    <xsl:value-of select="."/>
    <xsl:text>&#xA;</xsl:text>
</xsl:template>

  <xsl:template match="xs:alias">
    <xsl:value-of select="."/>
    <xsl:text>&#xA;</xsl:text>
  </xsl:template>
</xsl:stylesheet>
