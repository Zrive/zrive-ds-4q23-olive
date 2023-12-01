# EDA documentation of the parcelas

## Overview

This small document serves as a small explanation regarding the most relevant details of the EDA of the parcelas. Some of the data can be compared when getting a shorter or longer timespan. At the moment, we compare considering 30 days.

1. Generated_muestreos: 
- 0 NA
- Contains only 8 different dates when the samples were introduced in the system

2. Codparcela: 
- 0 NA
- 5061 landplots are presented, of which 75% of them have at least 142 consecutive measurements during 30 days time period. 
- There are some that only present one measurement, which is the month before the last data record was collected. A bit more explanation on date field. 

3. Provincia: 
- 0 NA
- Both when counting the absolute measurements and the number of landplots independently, the % of each province is similar: Jaen ~30%, Cordoba ~22%, Granada ~20%. Huelva and Almeria are practically insignificant.

4. Municipio: 
- 0 NA
- Ubeda has the highest percentage of measurements done to a town, and it is as low as 4%, so generally the measurements are around many different towns. 

5. Fecha: 
- 0 NA
- Of all the valid measurements, 1% was collected in 2005 and 2021, and during the rest of the years a consistent 5 - 8% was collected (2006-2020). ç
- As for the monthly distribution, most measurements are recorded through March - November, yet in some years they start recording in January/February and in others they might end in December, but this never represents more than 1% of the total year measurements. 

6. Campaña: 
- 0 NA
- It should be the same as the year of the Fecha measurement, yet there are some slight differences/discrepancies (less than 0.1%). 
- Consistently, in 2005 6 landplots where studied and in 2021 828 landplots where studied. During 2006 - 2020, consistently 1000 - 1400 landplots are being studied. In 2006, 1396 landplots where added in the study, and from then on around 100 - 400 new landplots are added every year, at the same time that other ones are discarded. 

7. Poligono: 
- 24 NA
- Some codparcelas do not have any poligono because of their nomenclature (R-879, RAIF-5). There are less poligonos than municipios, so one poligono can contain several towns. 

8. Parcela: 
- 29 NA
- It seems as if for each poligono, there is a parcela number 1, number 2...

9. Recinto: 
- 1601 NA
- Most of them is either 0, 1 or NA (~80%).

10. Subrecinto: 
- 96179 NA
- Most of them is either 0, 1 or NA (~80%).

26. Generated_parcelas: 
- 3157 NA
- Non NA values are egarding to 2 input dates to the system: 2020-04-23 and 2021-04-05.

27. 102_coordenada_x_(utm): 
- 9529 NA
- Total of 5140 X coordinates. We have 5061 landplots, so that means in 79 occasions there is a change of X coordinate in a landplot. 

28. 103_coordenada_y_(utm): 
- 9621 NA
-Total of 5098 Y coordinates. We have 5061 landplots, so that means in 37 occasions there is a change of Y coordinate in a landplot.

29. 104_altitud_(m): 
- 176080 NA
- There are only 571 values for altitude. 

30. 105_comarca: 
- 3558 NA
- Most present category is Norte o Antequera, with 10%. 

31. 201_superf_cultivada_en_la_parcela_agrícola_(ha): 
- 46427 NA
- Even though there is a maximum of 35781 ha which seems an outlier or error, 50% of the landplots fall within 6 ha and 75% of them fall within  21 ha. 

32. 202_pendiente_(%): 
- 185343 NA
- Maximum is 64%, Q3 is 15, median is 8 and Q1 is 4. 

33. 203_orientación: 
- 310032 NA (~60%)
- The non null ones should be homogeneized accordingly ("SUR ESTE" and "SURESTE"). 

34. 204_textura_del_suelo: 
- 292643 NA (~50%)
- The non null ones should be homogeneized accordingly ("FRANCO-ARCILLOSO", "Franco-arcilloso", "Franco arcilloso"). 

35. 206_secano_/_regadío: 
- 62789 NA
- There are around 53% of the landplots that are secano and 47% that are regadio.
- Needs homogeneizing ("SECANO", "secano")

36. 211_utilización_de_cubierta_vegetal: 
- 148666 NA
- 43% have some vegetation covering and 56% don´t. 
- Needs homogeneizing ("Si", "SI")

37. 212_tipo_de_cubierta_vegetal:
- 301766 NA
- Needs homogeneizing ("Instalada con siembra", "INSTALADA CON SIEMBRA")

38. 214_cultivo_asociado/otro_aprovechamiento:
- 478890 NA
- Needs homogeneizing ("Si", "SI")

39. 301_marco_(m_x_m)
- 75821 NA
- It is a space distribution of the olive trees in the parcela
- Requires homogeneizing, ("10 X 10", "10x10", "10*10")

40. 302_densidad_(plantas/ha):
- 73764 NA
- It defines the density of olive trees, like plants/hectar. 1 hectar = 10000 m2
- This should be redundant or deeply related to the marco: 10x10 m means that in 1 hectar we have 100 olive trees, 9x9m --> 123 olive trees, 8x8 m --> 156 olive trees... suspiciously, it matches numbers in the result.

41. 303_nº_pies_por_árbol:
- 127049 NA
- It defines the number of trunks that come from the same olive tree, approximately
- It may be a false friend because in some webpage it is synonym for density (plants / ha), and some numbers in this column are very high for it to be the trunks.

42. 304_formación:
- 391156 NA
- How the tree is formed and perhaps cutted.
- Different categories: Vaso, Tresbolillo... (needs homogeneizing "VASO" "vaso")

43. 305_diámetro_de_copa_(m):
- 422610 NA
- Measurement of the upper part, branches leaves etc.
- Maximum of 10000 (clearly wrong), Q1 = 4 m, Median = 4 m, Q3 = 5.5 m

44. 308_variedad_principal:
- 51969 NA
- Homogeneization + grouping of trees ("Picual, Marteño", "PICUAL / MARTEÑO", "PICUAL", for instance.)

45. 311_fecha_de_plantación_variedad_principal:
- 402952 NA
- Most of the dates are in the period 1900 - 2000 (not from 2000 onwards)

46. 208_riego:_procedencia_del_agua:
- 408955 NA
- Mixes specific locations ("EMBALSE LA BOLERA", "C.R. los Barrancos") with non specific things at all "Pozo". "Pozo" & "POZO" are the most present, +50% of the non-NA values

47. 209_riego:_calidad_del_agua:
- 427275 NA
- Homogeneization ("APTAS PARA EL RIEGO','Aptas para riego','APTAS PARA RIEGO'")
- +90% of the labeled ones are "aptas para el riego"

48. 313_variedad_secundaria:
- 454577 NA
- Homogeneization + grouping of trees ("Picual, Marteño", "PICUAL / MARTEÑO", "PICUAL", for instance.)

49. 107_zona_homogénea
- 204781 NA
- Some look like RAIF biological zones (GR/OL/07), some look like routes/places (Olivar de Campiña)...

50. 120_zona_biológica_raif:

- 7303 NA
- The whole zone is defined by a code and a place, like 'JA/OL/02 LOMA ALTA'

51. 401_estación_climática_asociada:
- 156637 NA
- It includes the nearest weather station: some of them are in the RAIF network of weather stations and others are not in this network. For more info: https://helvia.uco.es/bitstream/handle/10396/22714/tfm_laura_rodriguez_navarro.pdf
- It needs homogeneizing too, e.g. maybe "JA012" is referred as "JAO12", or BAEZA09

52. 402_sensor_climático_asociado:
- 379136 NA
- It is a mix between the location, RAIF code, variable 51...

53. 207_riego:_sistema_usual_de_riego:
- 368102 NA
- It needs homogeneizing too, e.g. "Localizado gotero", "LOCALIZADO GOTERO", null values may be "ninguno" (after all, all secano olive trees should be "ninguno")...

54. 108_u_h_c_a_la_que_pertenece:
- 250324 NA
- Some kind of weather area parameter that is kind of redundant and a mix of the previous variables

55. 316_fecha_de_plantación_variedad_secundaria:
- 516075 NA
- As with previous date, most are between 1900 - 2000.

56. 315_patrón_variedad_secundaria:
- 519433 NA
- Same family of values as  48.313_variedad_secundaria, what might the difference be?

57. 317_%_superficie_ocupada_variedad_secundaria:
- 515049 NA
- Maximum value of surface of secondary species is 60%, Q1 is 10%, median is 25%, q3 is 34%

58. 306_altura_de_copa_(m):
- 468536 NA
- Maximum is 1.8e+7 (clearly wrong), q1 = 3, median = 3, q3 = 3.5

59. 310_patrón_variedad_principal:
- 410071 NA
- Same family of values as  308_variedad_principal, what might the difference be?

60. 411_representa_a_la_u_h_c_(si/no):
- 357609 NA
- Of the non-null values, 99.9% is "SI"

61. 109_sistema_para_el_cumplimiento_gestión_integrada:
- 400111 NA
- Of the non-nulls, >90% have "Produccion Integrada"
