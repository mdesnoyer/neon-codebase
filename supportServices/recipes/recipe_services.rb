#Get the latest commit from git --> Make test & release --> setup monitoring & init scripts --> start services --> start monit
bash "Setup Services App" do
  user "root"
  code <<-EOH
  echo "start services setup" 
  cd /opt/neon/
  git stash
  git pull
  source enable_env
  make test
  EOH
end
