package projectconfig

// LoadOptions selects project config files and the Atlas env.
type LoadOptions struct {
	PtahPath  string
	AtlasPath string
	EnvName   string
	AtlasVars []string
}

// Load reads Ptah and Atlas project config files and merges them with the
// documented precedence: atlas.hcl beats ptah.yaml. An explicit PtahPath is
// required to exist; an empty PtahPath uses the optional conventional
// ./ptah.yaml path.
func Load(opts LoadOptions) (Config, error) {
	configs, err := LoadCollection(opts)
	if err != nil {
		return Config{}, err
	}
	return singularAtlasConfig(configs, opts.EnvName)
}

// LoadCollection reads Ptah and Atlas project config files and returns every
// selected Atlas instance merged over the Ptah config.
func LoadCollection(opts LoadOptions) ([]Config, error) {
	ptahPath := opts.PtahPath
	if ptahPath == "" {
		ptahPath = PtahFileName
	}
	atlasPath := opts.AtlasPath
	if atlasPath == "" {
		atlasPath = AtlasFileName
	}

	ptahSource := discoveredPtahConfig
	if opts.PtahPath != "" {
		ptahSource = explicitPtahConfig
	}

	ptah, err := loadPtahFile(ptahPath, opts.EnvName, ptahSource)
	if err != nil {
		return nil, err
	}
	atlas, err := LoadAtlasFileCollectionWithOptions(atlasPath, AtlasLoadOptions{
		EnvName: opts.EnvName,
		Vars:    opts.AtlasVars,
	})
	if err != nil {
		return nil, err
	}
	configs := make([]Config, len(atlas))
	for i := range atlas {
		configs[i] = Merge(ptah, atlas[i])
	}
	return configs, nil
}
