defmodule Ecto.Adapters.DynamoDB.QueryParser.Test do
  use ExUnit.Case

  alias Ecto.Adapters.DynamoDB.QueryParser
  import Ecto.Query

  describe "#extract_lookup_fields" do
    test "extract one where field" do
      query = from(Ecto.Migration.SchemaMigration, where: [version: 20_200_110_134_036])
      res = QueryParser.extract_lookup_fields(query.wheres, [])

      assert res == [{"version", {20_200_110_134_036, :==}}]
    end

    test "extract one where field with pinning" do
      date = ~U[2020-01-17 06:11:53.040666Z]
      version = 20_200_110_134_036

      query =
        from(p in Ecto.Migration.SchemaMigration,
          where: p.version == type(^version, :integer) and p.updated_at > ^date
        )

      res = QueryParser.extract_lookup_fields(query.wheres, [version, date])

      assert res == [
               {"updated_at", {~U[2020-01-17 06:11:53.040666Z], :>}},
               {"version", {20_200_110_134_036, :==}}
             ]
    end

    test "placeholder in tagged query" do
      version = 20_200_110_134_036

      wheres = [
        %Ecto.Query.BooleanExpr{
          expr:
            {:==, [],
             [
               {{:., [], [{:&, [], [0]}, :version]}, [], []},
               %Ecto.Query.Tagged{
                 tag: :integer,
                 type: :integer,
                 value: {:^, [], [0]}
               }
             ]}
        }
      ]

      res = QueryParser.extract_lookup_fields(wheres, [version])

      assert res == [{"version", {20_200_110_134_036, :==}}]
    end
  end
end
