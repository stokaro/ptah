package projectconfig

// LoadOptions selects project config files and the Atlas env.
type LoadOptions struct {
	PtahPath  string
	AtlasPath string
	EnvName   string
}

// Load reads Ptah and Atlas project config files and merges them with the
// documented precedence: atlas.hcl beats ptah.yaml.
func Load(opts LoadOptions) (Config, error) {
	ptahPath := opts.PtahPath
	if ptahPath == "" {
		ptahPath = PtahFileName
	}
	atlasPath := opts.AtlasPath
	if atlasPath == "" {
		atlasPath = AtlasFileName
	}

	ptah, err := LoadPtahFile(ptahPath, opts.EnvName)
	if err != nil {
		return Config{}, err
	}
	atlas, err := LoadAtlasFile(atlasPath, opts.EnvName)
	if err != nil {
		return Config{}, err
	}
	return Merge(ptah, atlas), nil
}
