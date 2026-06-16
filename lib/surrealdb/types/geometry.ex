defmodule SurrealDB.Geometry do
  @moduledoc """
  A GeoJSON style geometry value.

  The `type` is one of `:point`, `:line`, `:polygon`, `:multi_point`, `:multi_line`,
  `:multi_polygon`, or `:collection`. For every type except `:collection`, `coordinates`
  holds the nested coordinate lists. For `:collection`, `coordinates` is a list of nested
  `Geometry` structs.

  Each type maps to a dedicated CBOR tag (88 through 94).

      iex> SurrealDB.Geometry.point(-0.118, 51.509)
      %SurrealDB.Geometry{type: :point, coordinates: [-0.118, 51.509]}
  """

  @enforce_keys [:type, :coordinates]
  defstruct [:type, :coordinates]

  @type geo_type ::
          :point | :line | :polygon | :multi_point | :multi_line | :multi_polygon | :collection
  @type t :: %__MODULE__{type: geo_type(), coordinates: list()}

  @doc "Builds a point from a longitude/latitude pair."
  @spec point(number(), number()) :: t()
  def point(longitude, latitude),
    do: %__MODULE__{type: :point, coordinates: [longitude, latitude]}

  @doc "Builds a line from a list of `[lon, lat]` points."
  @spec line([list()]) :: t()
  def line(points), do: %__MODULE__{type: :line, coordinates: points}

  @doc "Builds a polygon from a list of linear rings."
  @spec polygon([list()]) :: t()
  def polygon(rings), do: %__MODULE__{type: :polygon, coordinates: rings}

  @doc "Builds a geometry collection from a list of geometries."
  @spec collection([t()]) :: t()
  def collection(geometries), do: %__MODULE__{type: :collection, coordinates: geometries}
end
