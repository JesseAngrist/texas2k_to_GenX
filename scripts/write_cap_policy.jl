using CSV
using DataFrames
using Bijections
using Serialization

const CAP_RES_MARGIN = 0.1375   # 13.75% – ERCOT planning reserve margin

function write_cap_policy(
    policies_out::String = "../case/inputs/policies",
)
    mkpath(policies_out)

    bus_node_bij = deserialize("busToGenXNode.jls")
    zones = sort(unique(collect(values(bus_node_bij))))

    rows = [(
        Region_description = "TX",
        Network_zones      = "z$(z)",
        CapRes_1           = CAP_RES_MARGIN,
    ) for z in zones]

    df = DataFrame(rows)
    CSV.write(joinpath(policies_out, "Capacity_reserve_margin.csv"), df)
    println("Capacity_reserve_margin.csv written: $(nrow(df)) zones, CapRes_1 = $CAP_RES_MARGIN")
end

# ---------------------------------------------------------------------------
# ERCOT capacity credit (ELCC) derating factors for CapRes_1
# ---------------------------------------------------------------------------
const DERATING_THERMAL = 0.95   # ERCOT CDR: dispatchable thermal
const DERATING_WIND    = 0.147  # ERCOT 4×6 peak-period average (2024 CDR)
const DERATING_SOLAR   = 0.481  # ERCOT summer-peak average (2024 CDR)
const DERATING_BESS    = 0.80   # ERCOT ELCC for ≥3-hr storage (2024)
const DERATING_HYDRO   = 1.0    # Fully dispatchable – full capacity credit

function derating_for_vre(resource_name::AbstractString)
    prefix = split(resource_name, "_")[1]
    if prefix == "WND"
        return DERATING_WIND
    elseif prefix == "SUN"
        return DERATING_SOLAR
    else
        @warn "Unknown VRE prefix for '$resource_name'; defaulting to wind derating"
        return DERATING_WIND
    end
end

function write_cap_resource_policy(
    resources_in::String  = "../case/inputs/resources",
    resources_out::String = "../case/inputs/resources/policy_assignments",
)
    mkpath(resources_out)

    rows = NamedTuple{(:Resource, :Derating_Factor_1), Tuple{String, Float64}}[]

    # Thermal – all get 0.95
    thermal_path = joinpath(resources_in, "Thermal.csv")
    if isfile(thermal_path)
        df = CSV.read(thermal_path, DataFrame)
        for r in df[!, "Resource"]
            push!(rows, (Resource = r, Derating_Factor_1 = DERATING_THERMAL))
        end
        println("  Thermal: $(nrow(df)) resources → $DERATING_THERMAL")
    else
        @warn "Thermal.csv not found at $thermal_path"
    end

    # VRE – WND vs SUN determined by name prefix
    vre_path = joinpath(resources_in, "VRE.csv")
    if isfile(vre_path)
        df = CSV.read(vre_path, DataFrame)
        for r in df[!, "Resource"]
            push!(rows, (Resource = r, Derating_Factor_1 = derating_for_vre(r)))
        end
        n_wnd = count(r -> startswith(r, "WND"), df[!, "Resource"])
        n_sun = count(r -> startswith(r, "SUN"), df[!, "Resource"])
        println("  VRE: $n_wnd wind → $DERATING_WIND, $n_sun solar → $DERATING_SOLAR")
    else
        @warn "VRE.csv not found at $vre_path"
    end

    # Storage – all get 0.80
    stor_path = joinpath(resources_in, "Storage.csv")
    if isfile(stor_path)
        df = CSV.read(stor_path, DataFrame)
        for r in df[!, "Resource"]
            push!(rows, (Resource = r, Derating_Factor_1 = DERATING_BESS))
        end
        println("  Storage: $(nrow(df)) resources → $DERATING_BESS")
    else
        @warn "Storage.csv not found at $stor_path"
    end

    # Hydro – full credit
    hydro_path = joinpath(resources_in, "Hydro.csv")
    if isfile(hydro_path)
        df = CSV.read(hydro_path, DataFrame)
        for r in df[!, "Resource"]
            push!(rows, (Resource = r, Derating_Factor_1 = DERATING_HYDRO))
        end
        println("  Hydro: $(nrow(df)) resources → $DERATING_HYDRO")
    else
        @info "Hydro.csv not found at $hydro_path (skipping)"
    end

    out_df = DataFrame(rows)
    out_path = joinpath(resources_out, "Resource_capacity_reserve_margin.csv")
    CSV.write(out_path, out_df)
    println("Resource_capacity_reserve_margin.csv written: $(nrow(out_df)) resources → $out_path")
end

write_cap_policy()
write_cap_resource_policy()
