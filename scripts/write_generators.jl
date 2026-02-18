using CSV
using DataFrames
using Bijections
using Serialization
using Statistics


#=
Cost and performance data sources (NREL ATB 2024, 2025 technology year, Moderate scenario):
  https://atb.nrel.gov/electricity/2024/index  (file: NLR2024ATB.xlsx)

  Annualized CAPEX = OCC ($/kW) × CRF, where CRF = r(1+r)^n / ((1+r)^n - 1)
  Assumed discount rate r = 8.5% (utility/IPP mid-range)
  Assumed lifetimes: NG CC/CT 30yr, Coal/Nuclear 40yr, Wind 25yr, Solar 30yr,
                     Battery 15yr, Hydro 50yr

  Nuclear: ATB 2024 has no 2025 data; 2030 Moderate values used as proxy.
  DFO (Oil): No ATB entry; NG CT costs used as proxy (similar plant type). New_Build = 0.
  Hydro new-build: ATB Non-Powered Dam (NPD) Class 1 used; New_Build = 0
    because new dam construction faces prohibitive permitting constraints.

=#

# --- NREL ATB 2024, Moderate scenario, 2025 technology year ---
# All costs in USD 2022. FOM in $/MW-yr, VOM in $/MWh, CAPEX in $/MW-yr (annualized).

# Natural Gas Combined Cycle – 2x1 F-Frame, >200 MW (ATB: CCAvgCF, Moderate)
#   OCC $1,213/kW × CRF(30yr,8.5%)=0.09306 → $112,900/MW-yr
const NG_CCGT_HR_CENTRAL    = 6.316   # MMBtu/MWh
const NG_CCGT_CAPEX         = 112900.0# $/MW-yr (annualized OCC)
const NG_CCGT_FOM           = 33500.0 # $/MW-yr
const NG_CCGT_VOM           = 2.12    # $/MWh
const NG_CCGT_MIN_POWER     = 0.40    # fraction of nameplate
const NG_CCGT_RAMP          = 0.64    # fraction of capacity per hour

# Natural Gas Combustion Turbine – F-Frame 233 MW, ≤200 MW (ATB: CTAvgCF, all scenarios equal)
#   OCC $1,084/kW × CRF(30yr,8.5%)=0.09306 → $100,900/MW-yr
const NG_CT_HR_CENTRAL      = 9.717   # MMBtu/MWh
const NG_CT_CAPEX           = 100900.0# $/MW-yr (annualized OCC)
const NG_CT_FOM             = 25700.0 # $/MW-yr
const NG_CT_VOM             = 6.94    # $/MWh
const NG_CT_MIN_POWER       = 0.30
const NG_CT_RAMP            = 0.90
const NG_CCGT_THRESHOLD_MW  = 200.0   # MW, separates CCGT from CT

# Coal – Supercritical PC 650 MW (ATB: newAvgCF, Moderate)
#   OCC $3,149/kW × CRF(40yr,8.5%)=0.08838 → $278,300/MW-yr
const COAL_HR_CENTRAL       = 8.419   # MMBtu/MWh
const COAL_CAPEX            = 278300.0# $/MW-yr (annualized OCC)
const COAL_FOM              = 85300.0 # $/MW-yr
const COAL_VOM              = 9.18    # $/MWh
const COAL_MIN_POWER        = 0.40
const COAL_RAMP             = 0.30

# Distillate Fuel Oil – no ATB entry; NG CT costs used as proxy
const OIL_HR_CENTRAL        = 11.0    # MMBtu/MWh (EIA/EPA reference for DFO peakers)
const OIL_CAPEX             = 100900.0# $/MW-yr (proxy: NG CT OCC)
const OIL_FOM               = 25700.0 # $/MW-yr (proxy: NG CT FOM)
const OIL_VOM               = 6.94    # $/MWh (proxy: NG CT VOM)
const OIL_MIN_POWER         = 0.25
const OIL_RAMP              = 0.90

# Nuclear – Large LWR (ATB: nuclearLarge, Moderate, 2030 proxy; no 2025 entry)
#   OCC $5,750/kW × CRF(40yr,8.5%)=0.08838 → $508,200/MW-yr
const NUC_HR_CENTRAL        = 10.497  # MMBtu/MWh
const NUC_CAPEX             = 508200.0# $/MW-yr (annualized OCC, 2030 proxy)
const NUC_FOM               = 175000.0# $/MW-yr
const NUC_VOM               = 2.80    # $/MWh
const NUC_MIN_POWER         = 0.90    # runs at baseload
const NUC_RAMP              = 0.05

# Wind – Land-Based (ATB: Class 4/5 avg OCC $1,380/kW, Moderate)
#   OCC $1,380/kW × CRF(25yr,8.5%)=0.09770 → $134,800/MW-yr
const WIND_CAPEX            = 134800.0# $/MW-yr (annualized OCC)
const WIND_FOM              = 31250.0 # $/MW-yr
const WIND_VOM              = 0.0     # $/MWh

# Utility-Scale PV (ATB: all resource classes same cost, Moderate)
#   OCC $1,323/kW × CRF(30yr,8.5%)=0.09306 → $123,100/MW-yr
const SOLAR_CAPEX           = 123100.0# $/MW-yr (annualized OCC)
const SOLAR_FOM             = 21000.0 # $/MW-yr
const SOLAR_VOM             = 0.0     # $/MWh

# Conventional Hydropower – Non-Powered Dam Class 1 (ATB: NPD1, Moderate)
#   OCC $3,045/kW × CRF(50yr,8.5%)=0.08647 → $263,300/MW-yr
#   New_Build = 0: new dam construction not feasible in practice
const HYDRO_CAPEX           = 263300.0# $/MW-yr (annualized OCC, for reference only)
const HYDRO_FOM             = 92000.0 # $/MW-yr
const HYDRO_VOM             = 0.0     # $/MWh
const HYDRO_MIN_POWER       = 0.10    # fraction of nameplate (minimum environmental flow)
const HYDRO_RAMP            = 0.50    # fraction of capacity per hour
const HYDRO_ENERGY_RATIO    = 12.0    # MWh/MW reservoir energy-to-power ratio

# Li-Ion Battery – 4-hr system (ATB: UtilityStorageWithSolar, Moderate)
#   Power OCC ~$460/kW × CRF(15yr,8.5%)=0.1204 → $55,400/MW-yr
#   Energy OCC $310/kWh × CRF → $37,300/MWh-yr
#   FOM split from ATB 2-hr/4-hr difference: ~$7,780/MW-yr + $7,750/MWh-yr
const BATT_CAPEX_MW         = 55400.0 # $/MW-yr  (annualized power OCC)
const BATT_CAPEX_MWH        = 37300.0 # $/MWh-yr (annualized energy OCC)
const BATT_FOM_MW           = 7780.0  # $/MW-yr  (power component FOM)
const BATT_FOM_MWH          = 7750.0  # $/MWh-yr (energy component FOM)
const BATT_VOM              = 0.0     # $/MWh discharged (ATB: $0/MWh)
const BATT_VOM_IN           = 0.0     # $/MWh charged
const BATT_EFF              = 0.92    # one-way charge/discharge efficiency
const BATT_SELF_DISCH       = 0.0     # self-discharge per hour (negligible for Li-Ion)
const BATT_MIN_DURATION     = 1       # hours
const BATT_MAX_DURATION     = 10      # hours (allow dispatch flexibility)
const BATT_HOURS_ASSUMED    = 4.0     # assume 4-hour battery for existing energy capacity

# -----------------------------------------------------------------------

"""
Scale an individual generator's heat rate proportionally to its IOB term,
anchored so that the median-IOB generator receives `central_hr`.
Clamps to ±50% of central to avoid extreme outliers.
"""
function calibrate_heat_rate(iob::Float64, median_iob::Float64, central_hr::Float64)
    if median_iob <= 0.0 || iob <= 0.0
        return central_hr
    end
    return clamp((iob / median_iob) * central_hr, 0.5 * central_hr, 1.5 * central_hr)
end

"""
Parse the short fuel-type code (e.g. "WND") from strings like "WND (Wind)".
"""
fuel_code(s) = split(strip(s), ' ')[1]

"""
Build a unique GenX resource name from fuel code, bus number, and generator ID.
"""
resource_name(fuel::AbstractString, bus::Int, id) = "$(fuel)_$(bus)_$(id)"

# -----------------------------------------------------------------------

function write_generators(
    tamu_dir::String       = "../TAMU_data",
    resources_out::String  = "../case/inputs/resources",
)
    mkpath(resources_out)

    bus_node_bij = deserialize("busToGenXNode.jls")

    # generators.csv has a title row ("Gen") before the header
    gens_df  = CSV.read(joinpath(tamu_dir, "generators.csv"),  DataFrame, header=2)

    # Only keep in-service generators
    filter!(row -> row.Status == "Closed", gens_df)

    # Parse short fuel codes from "FUEL (Description)" strings
    gens_df[!, :FuelCode] = fuel_code.(gens_df[!, "Fuel Type"])

    vre_codes = ["WND", "SUN"]
    vre_gens  = filter(row -> row.FuelCode in vre_codes, gens_df)

    # All buses that carry any generator – used for greenfield expansion nodes
    all_gen_buses = unique(gens_df[!, "Number of Bus"])

    # ---------------------------------------------------------------
    # Build Thermal.csv
    # ---------------------------------------------------------------
    thermal_codes = ["NG", "BIT", "DFO", "NUC", "OTH", "OBL"]
    therm_gens = filter(row -> row.FuelCode in thermal_codes, gens_df)

    # Helper to get non-zero IOB values for a sub-group
    valid_iob(sub) = [Float64(r.IOB) for r in eachrow(sub)
                      if !ismissing(r.IOB) && Float64(r.IOB) > 0.0]

    ng_all    = filter(r -> r.FuelCode == "NG",  therm_gens)
    ng_ccgt   = filter(r -> r["Max MW"] > NG_CCGT_THRESHOLD_MW, ng_all)
    ng_ct     = filter(r -> r["Max MW"] <= NG_CCGT_THRESHOLD_MW, ng_all)
    coal_gens = filter(r -> r.FuelCode == "BIT", therm_gens)
    oil_gens  = filter(r -> r.FuelCode == "DFO", therm_gens)

    med(v) = isempty(v) ? 1.0 : median(v)
    med_iob_ccgt = med(valid_iob(ng_ccgt))
    med_iob_ct   = med(valid_iob(ng_ct))
    med_iob_coal = med(valid_iob(coal_gens))
    med_iob_oil  = med(valid_iob(oil_gens))

    thermal_rows = []

    for row in eachrow(therm_gens)
        bus_num = row["Number of Bus"]
        !haskey(bus_node_bij, bus_num) && continue  # bus not mapped to a GenX zone

        node  = bus_node_bij[bus_num]
        code  = row.FuelCode
        name  = resource_name(code, bus_num, row.ID)

        max_mw = ismissing(row["Max MW"]) ? 0.0 : Float64(row["Max MW"])
        min_mw = ismissing(row["Min MW"]) ? 0.0 : Float64(row["Min MW"])
        iob    = (ismissing(row.IOB) || row.IOB == 0.0) ? 0.0 : Float64(row.IOB)

        # Ramp rate: MW/min → fraction of capacity per hour
        ramp_up_col = "Ramp Rate Up, MW/Minute"
        ramp_dn_col = "Ramp Rate Down, MW/Minute"
        ramp_up_mw_min = ismissing(row[ramp_up_col]) ? 0.0 : Float64(row[ramp_up_col])
        ramp_dn_mw_min = ismissing(row[ramp_dn_col]) ? 0.0 : Float64(row[ramp_dn_col])

        ramp_up_frac = max_mw > 0 ? clamp(abs(ramp_up_mw_min) * 60 / max_mw, 0.0, 1.0) : 1.0
        ramp_dn_frac = max_mw > 0 ? clamp(abs(ramp_dn_mw_min) * 60 / max_mw, 0.0, 1.0) : 1.0

        # Dispatch parameters by fuel type
        if code == "NG"
            if max_mw > NG_CCGT_THRESHOLD_MW
                hr    = calibrate_heat_rate(iob, med_iob_ccgt, NG_CCGT_HR_CENTRAL)
                capex = NG_CCGT_CAPEX; fom = NG_CCGT_FOM; vom = NG_CCGT_VOM
                min_pwr = NG_CCGT_MIN_POWER
                ramp_up_frac = max(ramp_up_frac, 0.1); ramp_up_frac = min(ramp_up_frac, NG_CCGT_RAMP)
                ramp_dn_frac = max(ramp_dn_frac, 0.1); ramp_dn_frac = min(ramp_dn_frac, NG_CCGT_RAMP)
            else
                hr    = calibrate_heat_rate(iob, med_iob_ct, NG_CT_HR_CENTRAL)
                capex = NG_CT_CAPEX; fom = NG_CT_FOM; vom = NG_CT_VOM
                min_pwr = NG_CT_MIN_POWER
                ramp_up_frac = max(ramp_up_frac, 0.1); ramp_up_frac = min(ramp_up_frac, NG_CT_RAMP)
                ramp_dn_frac = max(ramp_dn_frac, 0.1); ramp_dn_frac = min(ramp_dn_frac, NG_CT_RAMP)
            end
            fuel = "TX_NG"
        elseif code == "BIT"
            hr    = calibrate_heat_rate(iob, med_iob_coal, COAL_HR_CENTRAL)
            capex = COAL_CAPEX; fom = COAL_FOM; vom = COAL_VOM; min_pwr = COAL_MIN_POWER
            ramp_up_frac = clamp(ramp_up_frac, 0.05, COAL_RAMP)
            ramp_dn_frac = clamp(ramp_dn_frac, 0.05, COAL_RAMP)
            fuel = "TX_coal"
        elseif code == "DFO"
            hr    = calibrate_heat_rate(iob, med_iob_oil, OIL_HR_CENTRAL)
            capex = OIL_CAPEX; fom = OIL_FOM; vom = OIL_VOM; min_pwr = OIL_MIN_POWER
            ramp_up_frac = clamp(ramp_up_frac, 0.1, OIL_RAMP)
            ramp_dn_frac = clamp(ramp_dn_frac, 0.1, OIL_RAMP)
            fuel = "TX_oil"
        elseif code == "NUC"
            hr    = NUC_HR_CENTRAL
            capex = NUC_CAPEX; fom = NUC_FOM; vom = NUC_VOM; min_pwr = NUC_MIN_POWER
            ramp_up_frac = NUC_RAMP; ramp_dn_frac = NUC_RAMP
            fuel = "Nuclear"
        else  # OTH, OBL – treat as generic gas peaker
            hr    = NG_CT_HR_CENTRAL
            capex = NG_CT_CAPEX; fom = NG_CT_FOM; vom = NG_CT_VOM; min_pwr = 0.25
            ramp_up_frac = NG_CT_RAMP; ramp_dn_frac = NG_CT_RAMP
            fuel = "TX_NG"
        end

        min_pwr_actual = max_mw > 0 ? clamp(min_mw / max_mw, min_pwr, 1.0) : min_pwr

        push!(thermal_rows, (
            Resource                 = name,
            Zone                     = node,
            Model                    = 2,       # economic dispatch (no unit commitment per settings)
            New_Build                = 0,
            Can_Retire               = 0,
            Existing_Cap_MW          = round(max_mw, digits=2),
            Max_Cap_MW               = -1,
            Min_Cap_MW               = -1,
            Inv_Cost_per_MWyr        = capex,
            Fixed_OM_Cost_per_MWyr   = fom,
            Var_OM_Cost_per_MWh      = vom,
            Heat_Rate_MMBTU_per_MWh  = round(hr, digits=3),
            Fuel                     = fuel,
            Min_Power                = round(min_pwr_actual, digits=3),
            Ramp_Up_Percentage       = round(ramp_up_frac, digits=3),
            Ramp_Dn_Percentage       = round(ramp_dn_frac, digits=3),
            region                   = "TX",
            cluster                  = 1,
        ))
    end

    # Greenfield thermal nodes – one entry per technology per bus,
    # Existing_Cap_MW = 0 so GenX can build new capacity anywhere.
    gf_thermal = [
        ("NG_CT",   NG_CT_HR_CENTRAL,   NG_CT_CAPEX,   NG_CT_FOM,   NG_CT_VOM,   NG_CT_MIN_POWER,   NG_CT_RAMP,   "TX_NG"),
        ("NG_CCGT", NG_CCGT_HR_CENTRAL, NG_CCGT_CAPEX, NG_CCGT_FOM, NG_CCGT_VOM, NG_CCGT_MIN_POWER, NG_CCGT_RAMP, "TX_NG"),
        ("COAL",    COAL_HR_CENTRAL,    COAL_CAPEX,    COAL_FOM,    COAL_VOM,    COAL_MIN_POWER,    COAL_RAMP,    "TX_coal"),
        ("NUC",     NUC_HR_CENTRAL,     NUC_CAPEX,     NUC_FOM,     NUC_VOM,     NUC_MIN_POWER,     NUC_RAMP,     "Nuclear"),
    ]
    for bus_num in all_gen_buses
        !haskey(bus_node_bij, bus_num) && continue
        node = bus_node_bij[bus_num]
        for (tech, hr, capex, fom, vom, min_pwr, ramp, fuel) in gf_thermal
            push!(thermal_rows, (
                Resource                = "$(tech)_$(bus_num)_GF",
                Zone                    = node,
                Model                   = 2,
                New_Build               = 1,
                Can_Retire              = 0,
                Existing_Cap_MW         = 0.0,
                Max_Cap_MW              = -1,
                Min_Cap_MW              = -1,
                Inv_Cost_per_MWyr       = capex,
                Fixed_OM_Cost_per_MWyr  = fom,
                Var_OM_Cost_per_MWh     = vom,
                Heat_Rate_MMBTU_per_MWh = round(hr, digits=3),
                Fuel                    = fuel,
                Min_Power               = min_pwr,
                Ramp_Up_Percentage      = ramp,
                Ramp_Dn_Percentage      = ramp,
                region                  = "TX",
                cluster                 = 1,
            ))
        end
    end

    thermal_df = DataFrame(thermal_rows)
    CSV.write(joinpath(resources_out, "Thermal.csv"), thermal_df)
    println("Thermal.csv written: $(nrow(thermal_df)) generators ($(nrow(thermal_df) - length(all_gen_buses)*4) existing + $(length(all_gen_buses)*4) greenfield)")

    # ---------------------------------------------------------------
    # Build VRE.csv
    # ---------------------------------------------------------------
    vre_rows = []

    for row in eachrow(vre_gens)
        bus_num = row["Number of Bus"]
        !haskey(bus_node_bij, bus_num) && continue

        node    = bus_node_bij[bus_num]
        code    = row.FuelCode
        name    = resource_name(code, bus_num, row.ID)
        max_mw  = ismissing(row["Max MW"]) ? 0.0 : Float64(row["Max MW"])

        if code == "WND"
            capex = WIND_CAPEX; fom = WIND_FOM; vom = WIND_VOM
        else  # SUN
            capex = SOLAR_CAPEX; fom = SOLAR_FOM; vom = SOLAR_VOM
        end

        push!(vre_rows, (
            Resource               = name,
            Zone                   = node,
            Num_VRE_Bins           = 1,
            New_Build              = 0,
            Can_Retire             = 0,
            Existing_Cap_MW        = round(max_mw, digits=2),
            Max_Cap_MW             = -1,
            Min_Cap_MW             = -1,
            Inv_Cost_per_MWyr      = capex,
            Fixed_OM_Cost_per_MWyr = fom,
            Var_OM_Cost_per_MWh    = vom,
            region                 = "TX",
            cluster                = 1,
        ))
    end

    # Greenfield VRE nodes
    gf_vre = [
        ("WND", WIND_CAPEX,  WIND_FOM,  WIND_VOM),
        ("SUN", SOLAR_CAPEX, SOLAR_FOM, SOLAR_VOM),
    ]
    for bus_num in all_gen_buses
        !haskey(bus_node_bij, bus_num) && continue
        node = bus_node_bij[bus_num]
        for (fuel, capex, fom, vom) in gf_vre
            push!(vre_rows, (
                Resource               = "$(fuel)_$(bus_num)_GF",
                Zone                   = node,
                Num_VRE_Bins           = 1,
                New_Build              = 1,
                Can_Retire             = 0,
                Existing_Cap_MW        = 0.0,
                Max_Cap_MW             = -1,
                Min_Cap_MW             = -1,
                Inv_Cost_per_MWyr      = capex,
                Fixed_OM_Cost_per_MWyr = fom,
                Var_OM_Cost_per_MWh    = vom,
                region                 = "TX",
                cluster                = 1,
            ))
        end
    end

    vre_df = DataFrame(vre_rows)
    CSV.write(joinpath(resources_out, "VRE.csv"), vre_df)
    println("VRE.csv written: $(nrow(vre_df)) generators ($(nrow(vre_df) - length(all_gen_buses)*2) existing + $(length(all_gen_buses)*2) greenfield)")

    # ---------------------------------------------------------------
    # Build Storage.csv
    # ---------------------------------------------------------------
    stor_gens = filter(row -> row.FuelCode == "MWH", gens_df)
    storage_rows = []

    for row in eachrow(stor_gens)
        bus_num = row["Number of Bus"]
        !haskey(bus_node_bij, bus_num) && continue

        node   = bus_node_bij[bus_num]
        name   = resource_name("BESS", bus_num, row.ID)
        max_mw = ismissing(row["Max MW"]) ? 0.0 : Float64(row["Max MW"])

        push!(storage_rows, (
            Resource                     = name,
            Zone                         = node,
            Model                        = 1,     # symmetric charge/discharge
            New_Build                    = 0,
            Can_Retire                   = 0,
            Existing_Cap_MW              = round(max_mw, digits=2),
            Existing_Cap_MWh             = round(max_mw * BATT_HOURS_ASSUMED, digits=2),
            Max_Cap_MW                   = -1,
            Max_Cap_MWh                  = -1,
            Min_Cap_MW                   = -1,
            Min_Cap_MWh                  = -1,
            Inv_Cost_per_MWyr            = BATT_CAPEX_MW,
            Inv_Cost_per_MWhyr           = BATT_CAPEX_MWH,
            Fixed_OM_Cost_per_MWyr       = BATT_FOM_MW,
            Fixed_OM_Cost_per_MWhyr      = BATT_FOM_MWH,
            Var_OM_Cost_per_MWh          = BATT_VOM,
            Var_OM_Cost_per_MWh_In       = BATT_VOM_IN,
            Self_Disch                   = BATT_SELF_DISCH,
            Eff_Up                       = BATT_EFF,
            Eff_Down                     = BATT_EFF,
            Min_Duration                 = BATT_MIN_DURATION,
            Max_Duration                 = BATT_MAX_DURATION,
            region                       = "TX",
            cluster                      = 1,
        ))
    end

    # Greenfield storage nodes
    for bus_num in all_gen_buses
        !haskey(bus_node_bij, bus_num) && continue
        node = bus_node_bij[bus_num]
        push!(storage_rows, (
            Resource                = "BESS_$(bus_num)_GF",
            Zone                    = node,
            Model                   = 1,
            New_Build               = 1,
            Can_Retire              = 0,
            Existing_Cap_MW         = 0.0,
            Existing_Cap_MWh        = 0.0,
            Max_Cap_MW              = -1,
            Max_Cap_MWh             = -1,
            Min_Cap_MW              = -1,
            Min_Cap_MWh             = -1,
            Inv_Cost_per_MWyr       = BATT_CAPEX_MW,
            Inv_Cost_per_MWhyr      = BATT_CAPEX_MWH,
            Fixed_OM_Cost_per_MWyr  = BATT_FOM_MW,
            Fixed_OM_Cost_per_MWhyr = BATT_FOM_MWH,
            Var_OM_Cost_per_MWh     = BATT_VOM,
            Var_OM_Cost_per_MWh_In  = BATT_VOM_IN,
            Self_Disch              = BATT_SELF_DISCH,
            Eff_Up                  = BATT_EFF,
            Eff_Down                = BATT_EFF,
            Min_Duration            = BATT_MIN_DURATION,
            Max_Duration            = BATT_MAX_DURATION,
            region                  = "TX",
            cluster                 = 1,
        ))
    end

    storage_df = DataFrame(storage_rows)
    CSV.write(joinpath(resources_out, "Storage.csv"), storage_df)
    println("Storage.csv written: $(nrow(storage_df)) units ($(nrow(storage_df) - length(all_gen_buses)) existing + $(length(all_gen_buses)) greenfield)")

    # ---------------------------------------------------------------
    # Build Hydro.csv
    # ---------------------------------------------------------------
    hydro_gens = filter(row -> row.FuelCode == "WAT", gens_df)
    hydro_rows = []

    for row in eachrow(hydro_gens)
        bus_num = row["Number of Bus"]
        !haskey(bus_node_bij, bus_num) && continue

        node   = bus_node_bij[bus_num]
        name   = resource_name("WAT", bus_num, row.ID)
        max_mw = ismissing(row["Max MW"]) ? 0.0 : Float64(row["Max MW"])
        min_mw = ismissing(row["Min MW"]) ? 0.0 : Float64(row["Min MW"])
        min_pwr_actual = max_mw > 0 ? clamp(min_mw / max_mw, HYDRO_MIN_POWER, 1.0) : HYDRO_MIN_POWER

        push!(hydro_rows, (
            Resource                       = name,
            Zone                           = node,
            cluster                        = 1,
            New_Build                      = 0,     # new dam construction not feasible
            Can_Retire                     = 0,
            Existing_Cap_MW                = round(max_mw, digits=2),
            Max_Cap_MW                     = -1,
            Min_Cap_MW                     = -1,
            Inv_Cost_per_MWyr              = HYDRO_CAPEX, # NPD Class 1 refurbishment cost
            Fixed_OM_Cost_per_MWyr         = HYDRO_FOM,
            Var_OM_Cost_per_MWh            = HYDRO_VOM,
            Fuel                           = "None",
            Min_Power                      = round(min_pwr_actual, digits=3),
            Self_Disch                     = 0.0,
            Eff_Up                         = 1.0,
            Eff_Down                       = 1.0,
            Hydro_Energy_to_Power_Ratio    = HYDRO_ENERGY_RATIO,
            Min_Duration                   = 0,
            Max_Duration                   = 0,
            Ramp_Up_Percentage             = HYDRO_RAMP,
            Ramp_Dn_Percentage             = HYDRO_RAMP,
            LDS                            = 0,
            region                         = "TX",
        ))
    end

    hydro_df = DataFrame(hydro_rows)
    CSV.write(joinpath(resources_out, "Hydro.csv"), hydro_df)
    println("Hydro.csv written: $(nrow(hydro_df)) units")
end

write_generators()
