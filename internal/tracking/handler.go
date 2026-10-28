package tracking

import (
	"strconv"

	"github.com/gofiber/fiber/v2"
)

type Handler struct{ svc *Service }

func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

func (h *Handler) Register(r fiber.Router) {
	r.Get("/vehicles/:vehicle_id/location", h.latest)
	r.Get("/vehicles/:vehicle_id/history", h.history)
}

func (h *Handler) latest(c *fiber.Ctx) error {
	l, err := h.svc.Latest(c.UserContext(), c.Params("vehicle_id"))
	if err != nil {
		return err
	}
	return c.JSON(l)
}

func (h *Handler) history(c *fiber.Ctx) error {
	start, err := intQuery(c, "start")
	if err != nil {
		return err
	}
	end, err := intQuery(c, "end")
	if err != nil {
		return err
	}
	locs, truncated, err := h.svc.History(c.UserContext(), c.Params("vehicle_id"), start, end)
	if err != nil {
		return err
	}
	if truncated {
		c.Set("X-Result-Truncated", "true")
	}
	return c.JSON(locs)
}

func intQuery(c *fiber.Ctx, name string) (int64, error) {
	raw := c.Query(name)
	if raw == "" {
		return 0, invalid("query parameter %q is required", name)
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, invalid("query parameter %q must be an integer Unix time in seconds", name)
	}
	return n, nil
}
