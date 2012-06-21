dshell_env = {
  fiz = "faz",
  a = "b",
  zs = function() return 10 end
}

base_resources = cat {

}

base_master = {
  env = cat(dshell_env) {
    foo = "bar",
    biz = "baz",
  },
  resources = {
    -- Same
  },
  cpu = 1.5,
  memory = 50
}

base_node = cat {
  env = dshell_env
}

base_app = {}

distshell = yarn(cat(base_app) {
  name = "Distributed Shell",
  timeout = 86400,
  user = "josh",
  priority = 0,
  memory = 100,

  master = cat(base_master) {
    env = cat(base_master.env) {
      foo = "foo"
    },
    priority = 1,
    memory = 100,
  },

  node = base_node {

  }
})
