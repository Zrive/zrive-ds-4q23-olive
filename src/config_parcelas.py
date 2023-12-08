mean_thresholds_config = {
    "104_altitud_(m)": {"threshold": 2500, "is_int": True},  # No field should be higher than 2500 meters
    "201_superf_cultivada_en_la_parcela_agrícola_(ha)": {"threshold": 10000, "is_int": False},  # 100 km^2 maximum area
    "202_pendiente_(%)": {"threshold": 100, "is_int": True},  # No field should have a slope percentage higher than 100%
    "302_densidad_(plantas/ha)": {"threshold": 10000, "is_int": True},  # Maximum 1 plant per square meter
    "303_nº_pies_por_árbol": {"threshold": 100, "is_int": True},  # Number of stumps per tree, doesn't make sense to have more than a hundred
    "porcentaje_floracion": {"threshold": 100, "is_int": True},  # No field should have a flowering percentage higher than 100%
    "301_marco_(m_x_m)": {"threshold": 100, "is_int": False},  # Trees with spacing above 100x100 meters
}