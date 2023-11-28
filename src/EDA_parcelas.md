# EDA documentation of the parcelas

## Overview

This small document serves as a small explanation regarding the most relevant details of the EDA of the parcelas. Some of the data can be compared when getting a shorter or longer timespan. At the moment, we compare considering 30 days.

1. Generated_muestreos: contains only 8 different dates when the samples were introduced in the system.

2. Codparcela: 5061 landplots are presented, of which 75% of them have at least 142 consecutive measurements during 30 days time period. There are some that only present one measurement, which is the month before the last data record was collected. A bit more explanation on date field. 

3. Provincia: both when counting the absolute measurements and the number of landplots independently, the % of each province is similar: Jaen ~30%, Cordoba ~22%, Granada ~20%. Huelva and Almeria are practically insignificant.

4. Municipio: Ubeda has the highest percentage of measurements done to a town, and it is as low as 4%, so generally the measurements are around many different towns. 

5. Fecha: Of all the valid measurements, 1% was collected in 2005 and 2021, and during the rest of the years a consistent 5 - 8% was collected (2006-2020). As for the monthly distribution, most measurements are recorded through March - November, yet in some years they start recording in January/February and in others they might end in December, but this never represents more than 1% of the total year measurements. 

6. Campaña: It should be the same as the year of the Fecha measurement, yet there are some slight differences/discrepancies (less than 0.1%). Consistently, in 2005 6 landplots where studied and in 2021 828 landplots where studied. During 2006 - 2020, consistently 1000 - 1400 landplots are being studied. In 2006, 1396 landplots where added in the study, and from then on around 100 - 400 new landplots are added every year, at the same time that other ones are discarded. 

7. Poligono: first of the 4 digits that form a codparcela. Some codparcelas do not have any poligono because of their nomenclature (R-879, RAIF-5). There are less poligonos than municipios, so one poligono can contain several towns. 

8. Parcela: second of the 4 digit code that represents the landplot. It seems as if for each poligono, there is a parcela number 1, number 2...

9. Recinto: third of the 4 digit code that represents the landplot. Most of them is either 0, 1 or NA (~80%).

10. Subrecinto: fourth of the 4 digit code that represents the landplot. Most of them is either 0, 1 or NA (~80%).

26. Generated_parcelas: only null values and regarding to 2 input dates to the system: 2020-04-23 and 2021-04-05.

27. 102_coordenada_x_(utm): 9529 entries have null x coordinates, and the rest have a total of 5140 X coordinates. We have 5061 landplots, so that means in 79 occasions there is a change of X coordinate in a landplot. 

28. 103_coordenada_y_(utm): 9621 entries have null y coordinates, and the rest have a total of 5098 Y coordinates. We have 5061 landplots, so that means in 37 occasions there is a change of Y coordinate in a landplot.

29. 104_altitud_(m): 176080 entries have null values in their altitude, and there are only 571 values for altitude. 

30. 105_comarca: Most present category is Norte o Antequera, with 10%. 

31. 201_superf_cultivada_en_la_parcela_agrícola_(ha): even though there is a maximum of 35781 ha which seems an outlier or error, 50% of the landplots fall within 6 ha and 75% of them fall within  21 ha. 

32. 202_pendiente_(%): around 19000 entries have null values, and from the rest the maximum is 64%, Q3 is 15, median is 8 and Q1 is 4. 

33. 203_orientación: most values are null (~60%), and the ones not null should be homogeneized accordingly. 

34. 204_textura_del_suelo: most values are null (~50%), and the ones not null should be homogeneized accordingly.

35. 206_secano_/_regadío: there are around 53% of the landplots that are secano and 47% that are regadio.

36. 211_utilización_de_cubierta_vegetal: 43% have some vegetation covering and 56% don´t. 
