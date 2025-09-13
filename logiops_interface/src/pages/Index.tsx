import { useEffect, useState } from "react";
import { LoginForm } from "@/components/auth/LoginForm";
import { Navbar } from "@/components/layout/Navbar";
import SupervisorDashboard from "@/components/dashboard/SupervisorDashboard";
import { StockageDashboard } from "@/components/dashboard/StockageDashboard";
import { TransportDashboard } from "@/components/dashboard/TransportDashboard";
import CommandesDashboard from "@/components/dashboard/CommandesDashboard";

type ViewKey = "dashboard" | "commandes" | "stockage" | "transport";

const Index = () => {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [userProfile, setUserProfile] = useState<string>("");
  const [currentView, setCurrentView] = useState<ViewKey>("dashboard");

  // ancre à scroller après le rendu de la nouvelle vue
  const [pendingAnchor, setPendingAnchor] = useState<string | null>(null);

  const handleLogin = (profile: string) => {
    setUserProfile(profile);
    setIsAuthenticated(true);
    setCurrentView("dashboard");
  };

  const handleLogout = () => {
    setIsAuthenticated(false);
    setUserProfile("");
    setCurrentView("dashboard");
  };

  // Navigation depuis SupervisorDashboard
  const handleNavigate = (view: ViewKey, anchor?: string) => {
    setCurrentView(view);
    setPendingAnchor(anchor ?? null);
  };

  // Une fois la vue chargée, scroll vers l'ancre si fournie
  useEffect(() => {
    if (!pendingAnchor) return;

    const id = pendingAnchor.startsWith("#") ? pendingAnchor.slice(1) : pendingAnchor;

    // attendre le prochain paint pour que le DOM de la vue soit présent
    const raf = requestAnimationFrame(() => {
      const el = document.getElementById(id);
      if (el) {
        el.scrollIntoView({ behavior: "smooth", block: "start" });
      }
      setPendingAnchor(null);
    });

    return () => cancelAnimationFrame(raf);
  }, [currentView, pendingAnchor]);

  const renderDashboard = () => {
    if (currentView === "dashboard") {
      return userProfile === "superviseur"
        ? <SupervisorDashboard onNavigate={handleNavigate} />
        : renderSpecializedDashboard();
    }

    switch (currentView) {
      case "commandes":
        return <CommandesDashboard />;
      case "stockage":
        return <StockageDashboard />;
      case "transport":
        return <TransportDashboard />;
      default:
        return renderSpecializedDashboard();
    }
  };

  const renderSpecializedDashboard = () => {
    switch (userProfile) {
      case "commande":
        return <CommandesDashboard />;
      case "stockage":
        return <StockageDashboard />;
      case "transport":
        return <TransportDashboard />;
      default:
        return <SupervisorDashboard onNavigate={handleNavigate} />;
    }
  };

  if (!isAuthenticated) {
    return <LoginForm onLogin={handleLogin} />;
  }

  return (
    <div className="min-h-screen bg-background">
      <Navbar
        currentProfile={userProfile}
        currentView={currentView}
        onViewChange={setCurrentView}
        onLogout={handleLogout}
      />
      <main>
        {renderDashboard()}
      </main>
    </div>
  );
};

export default Index;
