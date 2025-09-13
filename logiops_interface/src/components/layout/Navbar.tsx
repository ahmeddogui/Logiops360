import { Button } from "@/components/ui/button";
import { 
  LayoutDashboard, 
  Package, 
  Warehouse, 
  Truck, 
  LogOut,
  Activity
} from "lucide-react";
import logoImage from "@/assets/logiops360-logo.png";

interface NavbarProps {
  currentProfile: string;
  currentView: string;
  onViewChange: (view: string) => void;
  onLogout: () => void;
}

const getProfileIcon = (profile: string) => {
  switch (profile) {
    case "commande": return <Package className="h-4 w-4" />;
    case "stockage": return <Warehouse className="h-4 w-4" />;
    case "transport": return <Truck className="h-4 w-4" />;
    case "superviseur": return <Activity className="h-4 w-4" />;
    default: return <LayoutDashboard className="h-4 w-4" />;
  }
};

const getProfileLabel = (profile: string) => {
  switch (profile) {
    case "commande": return "Gestionnaire Commande";
    case "stockage": return "Gestionnaire Stockage";
    case "transport": return "Gestionnaire Transport";
    case "superviseur": return "Superviseur";
    default: return "Utilisateur";
  }
};

export const Navbar = ({ currentProfile, currentView, onViewChange, onLogout }: NavbarProps) => {
  const getAvailableViews = () => {
    const baseViews = ["dashboard"];
    
    if (currentProfile === "superviseur") {
      return [...baseViews, "commandes", "stockage", "transport"];
    } else {
      return [...baseViews, currentProfile];
    }
  };

  const getViewLabel = (view: string) => {
    switch (view) {
      case "dashboard": return "Dashboard";
      case "commandes": return "Commandes";
      case "stockage": return "Stockage";
      case "transport": return "Transport";
      default: return view;
    }
  };

  return (
    <nav className="border-b bg-card" style={{ boxShadow: 'var(--shadow-card)' }}>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16">
          <div className="flex items-center space-x-8">
            <div className="flex items-center space-x-3">
              <img 
                src={logoImage} 
                alt="LogiOps 360 Logo" 
                className="h-8"
              />
            </div>
            
            <div className="flex space-x-1">
              {getAvailableViews().map((view) => (
                <Button
                  key={view}
                  variant={currentView === view ? "default" : "ghost"}
                  onClick={() => onViewChange(view)}
                  className="flex items-center space-x-2"
                >
                  {view === "dashboard" && <LayoutDashboard className="h-4 w-4" />}
                  {view === "commandes" && <Package className="h-4 w-4" />}
                  {view === "stockage" && <Warehouse className="h-4 w-4" />}
                  {view === "transport" && <Truck className="h-4 w-4" />}
                  <span>{getViewLabel(view)}</span>
                </Button>
              ))}
            </div>
          </div>
          
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2 text-sm text-muted-foreground">
              {getProfileIcon(currentProfile)}
              <span>{getProfileLabel(currentProfile)}</span>
            </div>
            
            <Button variant="outline" size="sm" onClick={onLogout}>
              <LogOut className="h-4 w-4 mr-2" />
              Déconnexion
            </Button>
          </div>
        </div>
      </div>
    </nav>
  );
};