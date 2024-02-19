package celestia

type DAConfig struct {
	Enable        bool   `koanf:"enable"`
	IsPoster      bool   `koanf:"enable"`
	Rpc           string `koanf:"rpc"`
	TendermintRPC string `koanf:"tendermint-rpc"`
	NamespaceId   string `koanf:"namespace-id"`
	AuthToken     string `koanf:"auth-token"`
}

func NewDAConfig(l1NodeUrl, sequencerInboxAddress, rpc, tRPC string, ns string, l1ConnectionAttemps int) (*DAConfig, error) {
	return &DAConfig{
		Enable:        true,
		Rpc:           rpc,
		TendermintRPC: tRPC,
		NamespaceId:   ns,
	}, nil
}
