#Get the latest commit from git --> Make test & release --> setup monitoring & init scripts --> start services --> start monit
bash "Setup Services App" do
  user "root"
  code <<-EOH
  echo "start services setup" 
  cd /opt/neon/
  git stash
  git checkout git@github.com:neon-lab/neon-codebase.git
  source enable_env
  make test >> /tmp/make.output
  EOH
end
