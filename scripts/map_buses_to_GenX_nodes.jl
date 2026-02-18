#using Pkg
#Pkg.add(["CSV", "DataFrames", "Bijections", "Serialization"])

using CSV
using DataFrames
using Bijections
using Serialization

function map_buses_to_GenX_labels()

    # deserialize and return the bijection if it exists:
    if isfile("busToGenXLabel.jls")
        bus_node_bij = deserialize("busToGenXLabel.jls")
        return bus_node_bij
    end

    buses_path = joinpath(@__DIR__, "..", "TAMU_data", "buses.csv")
    buses_df = CSV.read(buses_path, DataFrame)

    # create bus number to zone number bijection:
    bus_node_bij = Bijection{Int, Int}()
    # bus_node_bij[bus_number] = node_number
    # bus_node_bij(node_number) = bus_number

    for (node_number, row) in enumerate(eachrow(buses_df))
        bus_node_bij[row[:Number]] = node_number
    end

    return bus_node_bij

end