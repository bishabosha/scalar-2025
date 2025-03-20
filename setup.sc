//> using scala 3.7.0-RC1
//> using toolkit 0.7.0

val deps = (
  os.pwd / "upickle-nt",
  os.pwd / "query",
  os.pwd / "dataframe",
  os.pwd / "dataframe-laminar",
  os.pwd / "ops-mirror",
  os.pwd / "endpoints/core",
  os.pwd / "endpoints/fetchhttp",
  os.pwd / "endpoints/jdkhttp",
  os.pwd / "endpoints/fetchhttp-upickle",
  os.pwd / "endpoints/jdkhttp-upickle",
  os.pwd / "full-stack-example/core"
)

for path <- deps.toList do os.proc("make", "publishLocal").call(cwd = path)
